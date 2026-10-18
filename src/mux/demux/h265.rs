use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;

#[cfg(feature = "async")]
use tokio::fs::File as TokioFile;
#[cfg(feature = "async")]
use tokio::io::AsyncReadExt;

use crate::FourCc;
use crate::bitio::BitReader;
use crate::boxes::AnyTypeBox;
use crate::boxes::iso14496_12::{
    Btrt, Colr, HEVCDecoderConfiguration, HEVCNalu, HEVCNaluArray, Pasp, SampleEntry,
    VisualSampleEntry,
};

use super::super::MuxError;
use super::super::import::{
    SegmentedMuxSourceSegment, SegmentedMuxSourceSegmentData, SegmentedMuxSourceSpec, StagedSample,
};
use super::annexb_common::{
    AnnexBNal, AnnexBNalScanner, IndexedAnnexBTrack, nal_to_rbsp, push_unique_nal,
    read_bit_labeled, read_bits_u8_labeled, read_bits_u16_labeled, read_bits_u32_labeled,
    read_se_labeled, read_ue_labeled, skip_bits_labeled,
};
#[cfg(feature = "async")]
use super::container_common::read_segmented_bytes_async;
use super::container_common::read_segmented_bytes_sync;

const DVH1: FourCc = FourCc::from_bytes(*b"dvh1");

pub(in crate::mux) fn stage_annex_b_h265_sync(
    path: &Path,
    spec: &str,
) -> Result<IndexedAnnexBTrack, MuxError> {
    let mut file = File::open(path)?;
    let mut scanner = AnnexBNalScanner::default();
    let mut state = H265StageState::new();
    let mut chunk = [0_u8; 16 * 1024];

    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        scanner.push(&chunk[..read], |nal| stage_h265_nal(&mut state, nal))?;
    }
    scanner.finish(|nal| stage_h265_nal(&mut state, nal))?;
    finalize_h265_staged_track(path, state, spec)
}

pub(in crate::mux) fn stage_annex_b_h265_segmented_sync(
    path: &Path,
    file: &mut File,
    segments: &[SegmentedMuxSourceSegment],
    total_size: u64,
    spec: &str,
) -> Result<IndexedAnnexBTrack, MuxError> {
    let mut scanner = AnnexBNalScanner::default();
    let mut state = H265StageState::new();
    let mut offset = 0_u64;

    while offset < total_size {
        let read_len = usize::try_from((total_size - offset).min(16 * 1024))
            .map_err(|_| MuxError::LayoutOverflow("segmented H.265 scan chunk length"))?;
        let mut chunk = vec![0_u8; read_len];
        read_segmented_bytes_sync(
            file,
            segments,
            total_size,
            offset,
            &mut chunk,
            spec,
            "segmented H.265 scan chunk is truncated",
        )?;
        for nal in scanner.collect(&chunk) {
            stage_h265_nal_segmented(&mut state, nal)?;
        }
        offset = offset
            .checked_add(u64::try_from(read_len).unwrap())
            .ok_or(MuxError::LayoutOverflow("segmented H.265 scan offset"))?;
    }
    for nal in scanner.finish_collect() {
        stage_h265_nal_segmented(&mut state, nal)?;
    }
    finalize_h265_staged_track(path, state, spec)
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn stage_annex_b_h265_async(
    path: &Path,
    spec: &str,
) -> Result<IndexedAnnexBTrack, MuxError> {
    let mut file = TokioFile::open(path).await?;
    let mut scanner = AnnexBNalScanner::default();
    let mut state = H265StageState::new();
    let mut chunk = [0_u8; 16 * 1024];

    loop {
        let read = file.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        for nal in scanner.collect(&chunk[..read]) {
            stage_h265_nal(&mut state, nal)?;
        }
    }
    for nal in scanner.finish_collect() {
        stage_h265_nal(&mut state, nal)?;
    }
    finalize_h265_staged_track(path, state, spec)
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn stage_annex_b_h265_segmented_async(
    path: &Path,
    file: &mut TokioFile,
    segments: &[SegmentedMuxSourceSegment],
    total_size: u64,
    spec: &str,
) -> Result<IndexedAnnexBTrack, MuxError> {
    let mut scanner = AnnexBNalScanner::default();
    let mut state = H265StageState::new();
    let mut offset = 0_u64;

    while offset < total_size {
        let read_len = usize::try_from((total_size - offset).min(16 * 1024))
            .map_err(|_| MuxError::LayoutOverflow("segmented H.265 scan chunk length"))?;
        let mut chunk = vec![0_u8; read_len];
        read_segmented_bytes_async(
            file,
            segments,
            total_size,
            offset,
            &mut chunk,
            spec,
            "segmented H.265 scan chunk is truncated",
        )
        .await?;
        for nal in scanner.collect(&chunk) {
            stage_h265_nal_segmented(&mut state, nal)?;
        }
        offset = offset
            .checked_add(u64::try_from(read_len).unwrap())
            .ok_or(MuxError::LayoutOverflow("segmented H.265 scan offset"))?;
    }
    for nal in scanner.finish_collect() {
        stage_h265_nal_segmented(&mut state, nal)?;
    }
    finalize_h265_staged_track(path, state, spec)
}

struct H265StageState {
    vps_list: Vec<Vec<u8>>,
    sps_list: Vec<Vec<u8>>,
    pps_list: Vec<Vec<u8>>,
    samples: Vec<StagedSample>,
    sample_first_vcl_nals: Vec<Vec<u8>>,
    segments: Vec<SegmentedMuxSourceSegment>,
    current_sample_offset: Option<u64>,
    current_sample_first_vcl_nal: Option<Vec<u8>>,
    current_sample_size: u32,
    current_sync: bool,
    current_has_vcl: bool,
    saw_dolby_vision_nal: bool,
    logical_size: u64,
}

impl H265StageState {
    fn new() -> Self {
        Self {
            vps_list: Vec::new(),
            sps_list: Vec::new(),
            pps_list: Vec::new(),
            samples: Vec::new(),
            sample_first_vcl_nals: Vec::new(),
            segments: Vec::new(),
            current_sample_offset: None,
            current_sample_first_vcl_nal: None,
            current_sample_size: 0,
            current_sync: false,
            current_has_vcl: false,
            saw_dolby_vision_nal: false,
            logical_size: 0,
        }
    }

    fn finish_current_sample(&mut self) {
        if let Some(data_offset) = self.current_sample_offset.take() {
            self.samples.push(StagedSample {
                data_offset,
                data_size: self.current_sample_size,
                duration: 0,
                composition_time_offset: 0,
                is_sync_sample: self.current_sync,
            });
            self.sample_first_vcl_nals
                .push(self.current_sample_first_vcl_nal.take().unwrap_or_default());
            self.current_sample_size = 0;
            self.current_sync = false;
            self.current_has_vcl = false;
        }
    }

    fn append_sample_nal(
        &mut self,
        source_offset: u64,
        source_size: u32,
        is_sync_sample: bool,
        is_vcl: bool,
    ) -> Result<(), MuxError> {
        if self.current_sample_offset.is_none() {
            self.current_sample_offset = Some(self.logical_size);
        }
        let prefix = source_size.to_be_bytes();
        self.segments.push(SegmentedMuxSourceSegment {
            logical_offset: self.logical_size,
            data: SegmentedMuxSourceSegmentData::Prefix(prefix),
        });
        self.logical_size = self
            .logical_size
            .checked_add(4)
            .ok_or(MuxError::LayoutOverflow("raw H.265 transformed payload"))?;
        self.segments.push(SegmentedMuxSourceSegment {
            logical_offset: self.logical_size,
            data: SegmentedMuxSourceSegmentData::FileRange {
                source_offset,
                size: source_size,
            },
        });
        self.current_sample_size = self
            .current_sample_size
            .checked_add(
                4_u32
                    .checked_add(source_size)
                    .ok_or(MuxError::LayoutOverflow(
                        "raw H.265 transformed sample size",
                    ))?,
            )
            .ok_or(MuxError::LayoutOverflow("raw H.265 staged sample size"))?;
        self.logical_size = self
            .logical_size
            .checked_add(u64::from(source_size))
            .ok_or(MuxError::LayoutOverflow("raw H.265 transformed payload"))?;
        self.current_sync |= is_sync_sample;
        self.current_has_vcl |= is_vcl;
        Ok(())
    }

    fn append_sample_bytes(
        &mut self,
        bytes: Vec<u8>,
        is_sync_sample: bool,
        is_vcl: bool,
    ) -> Result<(), MuxError> {
        let source_size = u32::try_from(bytes.len())
            .map_err(|_| MuxError::LayoutOverflow("segmented H.265 NAL length"))?;
        if self.current_sample_offset.is_none() {
            self.current_sample_offset = Some(self.logical_size);
        }
        let prefix = source_size.to_be_bytes();
        self.segments.push(SegmentedMuxSourceSegment {
            logical_offset: self.logical_size,
            data: SegmentedMuxSourceSegmentData::Prefix(prefix),
        });
        self.logical_size = self
            .logical_size
            .checked_add(4)
            .ok_or(MuxError::LayoutOverflow(
                "segmented H.265 transformed payload",
            ))?;
        self.segments.push(SegmentedMuxSourceSegment {
            logical_offset: self.logical_size,
            data: SegmentedMuxSourceSegmentData::Bytes(bytes),
        });
        self.current_sample_size = self
            .current_sample_size
            .checked_add(
                4_u32
                    .checked_add(source_size)
                    .ok_or(MuxError::LayoutOverflow(
                        "segmented H.265 transformed sample size",
                    ))?,
            )
            .ok_or(MuxError::LayoutOverflow(
                "segmented H.265 staged sample size",
            ))?;
        self.logical_size = self
            .logical_size
            .checked_add(u64::from(source_size))
            .ok_or(MuxError::LayoutOverflow(
                "segmented H.265 transformed payload",
            ))?;
        self.current_sync |= is_sync_sample;
        self.current_has_vcl |= is_vcl;
        Ok(())
    }
}

fn stage_h265_nal(state: &mut H265StageState, nal: AnnexBNal) -> Result<(), MuxError> {
    if nal.bytes.len() < 2 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: "h265".to_string(),
            message: "H.265 NAL units must be at least two bytes long".to_string(),
        });
    }
    let nal_type = hevc_nal_type(&nal.bytes);
    let first_slice_segment = if is_hevc_vcl_nal_type(nal_type) {
        Some(h265_first_slice_segment_in_pic(&nal.bytes, "h265")?)
    } else {
        None
    };
    match nal_type {
        32 => push_unique_nal(&mut state.vps_list, nal.bytes),
        33 => push_unique_nal(&mut state.sps_list, nal.bytes),
        34 => push_unique_nal(&mut state.pps_list, nal.bytes),
        35 => state.finish_current_sample(),
        62 | 63 => {
            state.saw_dolby_vision_nal = true;
            let is_vcl = is_hevc_vcl_nal_type(nal_type);
            if first_slice_segment == Some(true) && state.current_has_vcl {
                state.finish_current_sample();
            }
            if is_vcl && state.current_sample_first_vcl_nal.is_none() {
                state.current_sample_first_vcl_nal = Some(nal.bytes.clone());
            }
            let nal_len = u32::try_from(nal.bytes.len())
                .map_err(|_| MuxError::LayoutOverflow("H.265 NAL length"))?;
            state.append_sample_nal(
                nal.source_offset,
                nal_len,
                is_hevc_sync_nal_type(nal_type),
                is_vcl,
            )?;
        }
        _ => {
            let is_vcl = is_hevc_vcl_nal_type(nal_type);
            if first_slice_segment == Some(true) && state.current_has_vcl {
                state.finish_current_sample();
            }
            if is_vcl && state.current_sample_first_vcl_nal.is_none() {
                state.current_sample_first_vcl_nal = Some(nal.bytes.clone());
            }
            let nal_len = u32::try_from(nal.bytes.len())
                .map_err(|_| MuxError::LayoutOverflow("H.265 NAL length"))?;
            state.append_sample_nal(
                nal.source_offset,
                nal_len,
                is_hevc_sync_nal_type(nal_type),
                is_vcl,
            )?;
        }
    }
    Ok(())
}

fn stage_h265_nal_segmented(state: &mut H265StageState, nal: AnnexBNal) -> Result<(), MuxError> {
    if nal.bytes.len() < 2 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: "h265".to_string(),
            message: "H.265 NAL units must be at least two bytes long".to_string(),
        });
    }
    let nal_type = hevc_nal_type(&nal.bytes);
    let first_slice_segment = if is_hevc_vcl_nal_type(nal_type) {
        Some(h265_first_slice_segment_in_pic(&nal.bytes, "h265")?)
    } else {
        None
    };
    match nal_type {
        32 => push_unique_nal(&mut state.vps_list, nal.bytes),
        33 => push_unique_nal(&mut state.sps_list, nal.bytes),
        34 => push_unique_nal(&mut state.pps_list, nal.bytes),
        35 => state.finish_current_sample(),
        62 | 63 => {
            state.saw_dolby_vision_nal = true;
            let is_vcl = is_hevc_vcl_nal_type(nal_type);
            if first_slice_segment == Some(true) && state.current_has_vcl {
                state.finish_current_sample();
            }
            if is_vcl && state.current_sample_first_vcl_nal.is_none() {
                state.current_sample_first_vcl_nal = Some(nal.bytes.clone());
            }
            state.append_sample_bytes(nal.bytes, is_hevc_sync_nal_type(nal_type), is_vcl)?;
        }
        _ => {
            let is_vcl = is_hevc_vcl_nal_type(nal_type);
            if first_slice_segment == Some(true) && state.current_has_vcl {
                state.finish_current_sample();
            }
            if is_vcl && state.current_sample_first_vcl_nal.is_none() {
                state.current_sample_first_vcl_nal = Some(nal.bytes.clone());
            }
            state.append_sample_bytes(nal.bytes, is_hevc_sync_nal_type(nal_type), is_vcl)?;
        }
    }
    Ok(())
}

fn finalize_h265_staged_track(
    path: &Path,
    mut state: H265StageState,
    spec: &str,
) -> Result<IndexedAnnexBTrack, MuxError> {
    state.finish_current_sample();
    if state.vps_list.is_empty() || state.sps_list.is_empty() || state.pps_list.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 input must include VPS, SPS, and PPS NAL units".to_string(),
        });
    }
    if state.samples.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 input contained parameter sets but no media samples".to_string(),
        });
    }

    let sps_info = parse_h265_sps_configuration(&state.sps_list[0], spec)?;
    let pps_info = parse_h265_pps_configuration(&state.pps_list[0], spec)?;
    let width = sps_info.width;
    let height = sps_info.height;
    let (timescale, sample_duration) = if let (Some(time_scale), Some(num_units_in_tick)) = (
        sps_info.timing_time_scale,
        sps_info.timing_num_units_in_tick,
    ) {
        (
            time_scale,
            num_units_in_tick
                .checked_mul(sps_info.timing_ticks_per_picture.unwrap_or(1))
                .ok_or(MuxError::LayoutOverflow(
                    "raw H.265 sample duration from SPS timing",
                ))?,
        )
    } else if state.samples.len() == 1 {
        (1, 1)
    } else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "multi-sample H.265 inputs currently require timing info in SPS VUI parameters"
                    .to_string(),
        });
    };
    for sample in &mut state.samples {
        sample.duration = sample_duration;
    }
    let sample_pocs = parse_h265_sample_pocs(
        &state.sample_first_vcl_nals,
        &sps_info,
        &pps_info,
        sample_duration,
        spec,
    );
    let mut source_edit_media_time = None;
    if let Some(sample_pocs) = sample_pocs {
        let mut min_composition_offset = i32::MAX;
        for (index, sample) in state.samples.iter_mut().enumerate() {
            let decode_time = i64::from(sample_duration)
                .checked_mul(
                    i64::try_from(index)
                        .map_err(|_| MuxError::LayoutOverflow("raw H.265 decode-time index"))?,
                )
                .ok_or(MuxError::LayoutOverflow("raw H.265 decode time"))?;
            let presentation_time = i64::from(sample_pocs[index]);
            let composition_offset = presentation_time
                .checked_sub(decode_time)
                .ok_or(MuxError::LayoutOverflow("raw H.265 composition offset"))?;
            sample.composition_time_offset = i32::try_from(composition_offset)
                .map_err(|_| MuxError::LayoutOverflow("raw H.265 composition offset"))?;
            min_composition_offset = min_composition_offset.min(sample.composition_time_offset);
        }
        if min_composition_offset < 0 {
            let shift = min_composition_offset
                .checked_neg()
                .ok_or(MuxError::LayoutOverflow(
                    "raw H.265 composition-offset shift",
                ))?;
            for sample in &mut state.samples {
                sample.composition_time_offset =
                    sample.composition_time_offset.checked_add(shift).ok_or(
                        MuxError::LayoutOverflow("raw H.265 shifted composition offset"),
                    )?;
            }
        }
        let mut min_presentation_time = i64::MAX;
        for (index, sample) in state.samples.iter().enumerate() {
            let decode_time = i64::from(sample_duration)
                .checked_mul(
                    i64::try_from(index)
                        .map_err(|_| MuxError::LayoutOverflow("raw H.265 decode-time index"))?,
                )
                .ok_or(MuxError::LayoutOverflow("raw H.265 decode time"))?;
            let presentation_time = decode_time
                .checked_add(i64::from(sample.composition_time_offset))
                .ok_or(MuxError::LayoutOverflow(
                    "raw H.265 presentation time after composition-offset shift",
                ))?;
            min_presentation_time = min_presentation_time.min(presentation_time);
        }
        if min_presentation_time > 0 {
            source_edit_media_time = Some(
                u64::try_from(min_presentation_time)
                    .map_err(|_| MuxError::LayoutOverflow("raw H.265 edit media time"))?,
            );
        }
    }
    let media_duration = staged_media_duration(&state.samples)
        .ok_or(MuxError::LayoutOverflow("raw H.265 media duration"))?;
    let sample_entry_type = if state.saw_dolby_vision_nal {
        DVH1
    } else {
        FourCc::from_bytes(*b"hvc1")
    };
    let display_width = display_track_width(width, sps_info.pixel_aspect_ratio.as_ref());
    let sample_entry_box = build_h265_sample_entry_box(H265SampleEntryInputs {
        sample_entry_type,
        width,
        height,
        sps_info: &sps_info,
        samples: &state.samples,
        media_duration,
        timescale,
        vps_list: &state.vps_list,
        sps_list: &state.sps_list,
        pps_list: &state.pps_list,
    })?;

    Ok(IndexedAnnexBTrack {
        segmented_source: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: state.segments,
            total_size: state.logical_size,
        },
        track_width: display_width,
        track_height: height,
        timescale,
        sample_entry_box,
        source_edit_media_time,
        samples: state.samples,
    })
}

struct H265SampleEntryInputs<'a> {
    sample_entry_type: FourCc,
    width: u16,
    height: u16,
    sps_info: &'a H265SpsInfo,
    samples: &'a [StagedSample],
    media_duration: u64,
    timescale: u32,
    vps_list: &'a [Vec<u8>],
    sps_list: &'a [Vec<u8>],
    pps_list: &'a [Vec<u8>],
}

fn build_h265_sample_entry_box(inputs: H265SampleEntryInputs<'_>) -> Result<Vec<u8>, MuxError> {
    let H265SampleEntryInputs {
        sample_entry_type,
        width,
        height,
        sps_info,
        samples,
        media_duration,
        timescale,
        vps_list,
        sps_list,
        pps_list,
    } = inputs;
    let mut sample_entry = VisualSampleEntry::default();
    sample_entry.set_box_type(sample_entry_type);
    sample_entry.sample_entry = SampleEntry {
        box_type: sample_entry_type,
        data_reference_index: 1,
    };
    sample_entry.width = width;
    sample_entry.height = height;
    sample_entry.horizresolution = 72_u32 << 16;
    sample_entry.vertresolution = 72_u32 << 16;
    sample_entry.frame_count = 1;
    sample_entry.depth = 0x0018;
    sample_entry.pre_defined3 = -1;

    let nalu_arrays = [(&vps_list, 32_u8), (&sps_list, 33_u8), (&pps_list, 34_u8)]
        .into_iter()
        .map(|(group, nalu_type)| -> Result<HEVCNaluArray, MuxError> {
            Ok(HEVCNaluArray {
                completeness: true,
                reserved: false,
                nalu_type,
                num_nalus: u16::try_from(group.len())
                    .map_err(|_| MuxError::LayoutOverflow("HEVC NAL count"))?,
                nalus: group
                    .iter()
                    .map(|nal| -> Result<HEVCNalu, MuxError> {
                        Ok(HEVCNalu {
                            length: u16::try_from(nal.len())
                                .map_err(|_| MuxError::LayoutOverflow("HEVC NAL length"))?,
                            nal_unit: nal.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut child_boxes = vec![super::super::mp4::encode_typed_box(
        &HEVCDecoderConfiguration {
            configuration_version: 1,
            general_profile_space: sps_info.general_profile_space,
            general_tier_flag: sps_info.general_tier_flag,
            general_profile_idc: sps_info.general_profile_idc,
            general_profile_compatibility: sps_info.general_profile_compatibility,
            general_constraint_indicator: sps_info.general_constraint_indicator,
            general_level_idc: sps_info.general_level_idc,
            min_spatial_segmentation_idc: 0,
            parallelism_type: 3,
            chroma_format_idc: sps_info.chroma_format_idc,
            bit_depth_luma_minus8: sps_info.bit_depth_luma_minus8,
            bit_depth_chroma_minus8: sps_info.bit_depth_chroma_minus8,
            avg_frame_rate: 0,
            constant_frame_rate: 0,
            num_temporal_layers: sps_info.num_temporal_layers,
            temporal_id_nested: sps_info.temporal_id_nested,
            length_size_minus_one: 3,
            num_of_nalu_arrays: u8::try_from(nalu_arrays.len())
                .map_err(|_| MuxError::LayoutOverflow("HEVC NAL array count"))?,
            nalu_arrays,
        },
        &[],
    )?];
    let pixel_aspect_ratio = sps_info.pixel_aspect_ratio.as_ref().map_or(
        Pasp {
            h_spacing: 1,
            v_spacing: 1,
        },
        |pixel_aspect_ratio| Pasp {
            h_spacing: pixel_aspect_ratio.h_spacing,
            v_spacing: pixel_aspect_ratio.v_spacing,
        },
    );
    child_boxes.push(super::super::mp4::encode_typed_box(
        &pixel_aspect_ratio,
        &[],
    )?);
    if let Some(color_info) = sps_info.color_info.as_ref() {
        child_boxes.push(super::super::mp4::encode_typed_box(
            &Colr {
                colour_type: FourCc::from_bytes(*b"nclx"),
                colour_primaries: color_info.colour_primaries,
                transfer_characteristics: color_info.transfer_characteristics,
                matrix_coefficients: color_info.matrix_coefficients,
                full_range_flag: color_info.full_range_flag,
                reserved: 0,
                profile: Vec::new(),
                unknown: Vec::new(),
            },
            &[],
        )?);
    }
    child_boxes.push(super::super::mp4::encode_typed_box(
        &build_btrt(samples, media_duration, timescale)?,
        &[],
    )?);

    super::super::mp4::encode_typed_box(&sample_entry, &child_boxes.concat())
}

fn build_btrt(
    samples: &[StagedSample],
    media_duration: u64,
    timescale: u32,
) -> Result<Btrt, MuxError> {
    if samples.is_empty() || media_duration == 0 || timescale == 0 {
        return Ok(Btrt::default());
    }

    let buffer_size_db = samples
        .iter()
        .map(|sample| sample.data_size)
        .max()
        .unwrap_or(0);
    let total_bytes = samples.iter().try_fold(0_u64, |sum, sample| {
        sum.checked_add(u64::from(sample.data_size))
            .ok_or(MuxError::LayoutOverflow("raw H.265 total sample bytes"))
    })?;
    let avg_bitrate = total_bytes
        .checked_mul(8)
        .and_then(|value| value.checked_mul(u64::from(timescale)))
        .ok_or(MuxError::LayoutOverflow("raw H.265 average bitrate"))?
        / media_duration;
    let avg_bitrate = avg_bitrate & !7;

    Ok(Btrt {
        buffer_size_db,
        max_bitrate: u32::try_from(avg_bitrate)
            .map_err(|_| MuxError::LayoutOverflow("raw H.265 maximum bitrate"))?,
        avg_bitrate: u32::try_from(avg_bitrate)
            .map_err(|_| MuxError::LayoutOverflow("raw H.265 average bitrate"))?,
    })
}

fn staged_media_duration(samples: &[StagedSample]) -> Option<u64> {
    let mut decode_time = 0_i64;
    let mut decode_end = 0_i64;
    let mut presentation_end = 0_i64;
    for sample in samples {
        let duration = i64::from(sample.duration);
        let sample_decode_end = decode_time.checked_add(duration)?;
        let sample_presentation_end = decode_time
            .checked_add(i64::from(sample.composition_time_offset))?
            .checked_add(duration)?;
        decode_end = decode_end.max(sample_decode_end);
        presentation_end = presentation_end.max(sample_presentation_end);
        decode_time = sample_decode_end;
    }
    u64::try_from(decode_end.max(presentation_end)).ok()
}

fn display_track_width(width: u16, pixel_aspect_ratio: Option<&H265PixelAspectRatio>) -> u16 {
    let Some(pixel_aspect_ratio) = pixel_aspect_ratio else {
        return width;
    };
    let numerator = u64::from(width)
        .saturating_mul(u64::from(pixel_aspect_ratio.h_spacing))
        .saturating_add(u64::from(pixel_aspect_ratio.v_spacing / 2));
    let display_width = numerator / u64::from(pixel_aspect_ratio.v_spacing);
    u16::try_from(display_width).unwrap_or(width)
}

fn parse_h265_pps_configuration(nal: &[u8], spec: &str) -> Result<H265PpsInfo, MuxError> {
    if nal.len() < 3 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 PPS NAL is too short".to_string(),
        });
    }
    let rbsp = nal_to_rbsp(&nal[2..]);
    let mut reader = BitReader::new(Cursor::new(rbsp));
    let _pps_pic_parameter_set_id = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _pps_seq_parameter_set_id = read_ue_labeled(&mut reader, spec, "H.265")?;
    Ok(H265PpsInfo {
        dependent_slice_segments_enabled_flag: read_bit_labeled(&mut reader, spec, "H.265")?,
        output_flag_present_flag: read_bit_labeled(&mut reader, spec, "H.265")?,
        num_extra_slice_header_bits: read_bits_u8_labeled(&mut reader, 3, spec, "H.265")?,
    })
}

fn parse_h265_sample_pocs(
    first_vcl_nals: &[Vec<u8>],
    sps_info: &H265SpsInfo,
    pps_info: &H265PpsInfo,
    sample_duration: u32,
    spec: &str,
) -> Option<Vec<i32>> {
    let mut pocs = Vec::with_capacity(first_vcl_nals.len());
    let mut prev_poc_lsb = 0_u32;
    let mut prev_poc_msb = 0_i32;
	let mut epoch: u64 = 0;
	let mut maximum_picture: Option<u64> = None;

    for nal in first_vcl_nals {
        let parsed =
            parse_h265_slice_poc(nal, sps_info, pps_info, prev_poc_lsb, prev_poc_msb, spec)?;
		// An IDR resets codec POC to zero, not the output presentation clock.
		// Keep each new IDR epoch after all pictures in the preceding GOP.
		if matches!(hevc_nal_type(nal), 19 | 20)
			&& let Some(maximum) = maximum_picture
		{
			epoch = maximum.checked_add(1)?;
		}
		let picture: u64 = epoch.checked_add(u64::from(parsed.poc))?;
		maximum_picture = Some(maximum_picture.map_or(picture, |maximum: u64| maximum.max(picture)));
        pocs.push(
            i32::try_from(picture)
                .ok()?
                .checked_mul(i32::try_from(sample_duration).ok()?)?,
        );
        prev_poc_lsb = parsed.poc_lsb;
        prev_poc_msb = parsed.poc_msb;
    }

    Some(pocs)
}

struct ParsedH265Poc {
    poc_lsb: u32,
    poc_msb: i32,
    poc: u32,
}

fn parse_h265_slice_poc(
    nal: &[u8],
    sps_info: &H265SpsInfo,
    pps_info: &H265PpsInfo,
    prev_poc_lsb: u32,
    prev_poc_msb: i32,
    spec: &str,
) -> Option<ParsedH265Poc> {
    if nal.len() < 3 {
        return None;
    }
    let nal_type = hevc_nal_type(nal);
    let idr_pic_flag = matches!(nal_type, 19 | 20);
    let rbsp = nal_to_rbsp(&nal[2..]);
    let mut reader = BitReader::new(Cursor::new(rbsp));
    let first_slice_segment_in_pic = read_bit_labeled(&mut reader, spec, "H.265").ok()?;
    if matches!(nal_type, 16..=23) {
        let _no_output_of_prior_pics_flag = read_bit_labeled(&mut reader, spec, "H.265").ok()?;
    }
    let _slice_pic_parameter_set_id = read_ue_labeled(&mut reader, spec, "H.265").ok()?;
    let dependent_slice_segment_flag =
        if !first_slice_segment_in_pic && pps_info.dependent_slice_segments_enabled_flag {
            read_bit_labeled(&mut reader, spec, "H.265").ok()?
        } else {
            false
        };
    if dependent_slice_segment_flag {
        return None;
    }
    if !first_slice_segment_in_pic {
        return None;
    }
    if pps_info.num_extra_slice_header_bits > 0 {
        let _ = read_bits_u8_labeled(
            &mut reader,
            usize::from(pps_info.num_extra_slice_header_bits),
            spec,
            "H.265",
        )
        .ok()?;
    }
    let _slice_type = read_ue_labeled(&mut reader, spec, "H.265").ok()?;
    if pps_info.output_flag_present_flag {
        let _pic_output_flag = read_bit_labeled(&mut reader, spec, "H.265").ok()?;
    }
    if sps_info.separate_colour_plane_flag {
        let _colour_plane_id = read_bits_u8_labeled(&mut reader, 2, spec, "H.265").ok()?;
    }
    if idr_pic_flag {
        return Some(ParsedH265Poc {
            poc_lsb: 0,
            poc_msb: 0,
            poc: 0,
        });
    }
    let poc_lsb = u32::from(
        read_bits_u16_labeled(
            &mut reader,
            usize::from(sps_info.log2_max_pic_order_cnt_lsb),
            spec,
            "H.265",
        )
        .ok()?,
    );
    let max_poc_lsb = 1_u32.checked_shl(u32::from(sps_info.log2_max_pic_order_cnt_lsb))?;
    let poc_msb = if poc_lsb < prev_poc_lsb && prev_poc_lsb - poc_lsb >= max_poc_lsb / 2 {
        prev_poc_msb.checked_add(i32::try_from(max_poc_lsb).ok()?)?
    } else if poc_lsb > prev_poc_lsb && poc_lsb - prev_poc_lsb > max_poc_lsb / 2 {
        prev_poc_msb.checked_sub(i32::try_from(max_poc_lsb).ok()?)?
    } else {
        prev_poc_msb
    };
    let poc = u32::try_from(i64::from(poc_msb) + i64::from(poc_lsb)).ok()?;
    Some(ParsedH265Poc {
        poc_lsb,
        poc_msb,
        poc,
    })
}

const fn hevc_nal_type(nal: &[u8]) -> u8 {
    (nal[0] >> 1) & 0x3F
}

const fn is_hevc_vcl_nal_type(nal_type: u8) -> bool {
    nal_type <= 31
}

const fn is_hevc_sync_nal_type(nal_type: u8) -> bool {
    matches!(nal_type, 16..=21)
}

fn h265_first_slice_segment_in_pic(nal: &[u8], spec: &str) -> Result<bool, MuxError> {
    if nal.len() < 3 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 VCL NAL is too short".to_string(),
        });
    }
    let rbsp = nal_to_rbsp(&nal[2..]);
    let mut reader = BitReader::new(Cursor::new(rbsp));
    read_bit_labeled(&mut reader, spec, "H.265")
}

struct H265SpsInfo {
    width: u16,
    height: u16,
    general_profile_space: u8,
    general_tier_flag: bool,
    general_profile_idc: u8,
    general_profile_compatibility: [bool; 32],
    general_constraint_indicator: [u8; 6],
    general_level_idc: u8,
    chroma_format_idc: u8,
    separate_colour_plane_flag: bool,
    bit_depth_luma_minus8: u8,
    bit_depth_chroma_minus8: u8,
    num_temporal_layers: u8,
    temporal_id_nested: u8,
    log2_max_pic_order_cnt_lsb: u8,
    timing_time_scale: Option<u32>,
    timing_num_units_in_tick: Option<u32>,
    timing_ticks_per_picture: Option<u32>,
    pixel_aspect_ratio: Option<H265PixelAspectRatio>,
    color_info: Option<H265ColorInfo>,
}

struct H265PpsInfo {
    dependent_slice_segments_enabled_flag: bool,
    output_flag_present_flag: bool,
    num_extra_slice_header_bits: u8,
}

struct H265PixelAspectRatio {
    h_spacing: u32,
    v_spacing: u32,
}

struct H265ColorInfo {
    colour_primaries: u16,
    transfer_characteristics: u16,
    matrix_coefficients: u16,
    full_range_flag: bool,
}

type H265VuiInfo = (
    Option<u32>,
    Option<u32>,
    Option<u32>,
    Option<H265PixelAspectRatio>,
    Option<H265ColorInfo>,
);

fn parse_h265_sps_configuration(nal: &[u8], spec: &str) -> Result<H265SpsInfo, MuxError> {
    if nal.len() < 3 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 SPS NAL is too short".to_string(),
        });
    }
    let rbsp = nal_to_rbsp(&nal[2..]);
    let mut reader = BitReader::new(Cursor::new(rbsp));
    let _sps_video_parameter_set_id = read_bits_u8_labeled(&mut reader, 4, spec, "H.265")?;
    let max_sub_layers_minus1 = read_bits_u8_labeled(&mut reader, 3, spec, "H.265")?;
    let temporal_id_nested = u8::from(read_bit_labeled(&mut reader, spec, "H.265")?);
    let general_profile_space = read_bits_u8_labeled(&mut reader, 2, spec, "H.265")?;
    let general_tier_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let general_profile_idc = read_bits_u8_labeled(&mut reader, 5, spec, "H.265")?;
    let mut general_profile_compatibility = [false; 32];
    for entry in &mut general_profile_compatibility {
        *entry = read_bit_labeled(&mut reader, spec, "H.265")?;
    }
    let mut general_constraint_indicator = [0_u8; 6];
    for entry in &mut general_constraint_indicator {
        *entry = read_bits_u8_labeled(&mut reader, 8, spec, "H.265")?;
    }
    let general_level_idc = read_bits_u8_labeled(&mut reader, 8, spec, "H.265")?;

    let mut sub_layer_profile_present_flags =
        Vec::with_capacity(usize::from(max_sub_layers_minus1));
    let mut sub_layer_level_present_flags = Vec::with_capacity(usize::from(max_sub_layers_minus1));
    for _ in 0..max_sub_layers_minus1 {
        sub_layer_profile_present_flags.push(read_bit_labeled(&mut reader, spec, "H.265")?);
        sub_layer_level_present_flags.push(read_bit_labeled(&mut reader, spec, "H.265")?);
    }
    if max_sub_layers_minus1 > 0 {
        for _ in max_sub_layers_minus1..8 {
            skip_bits_labeled(&mut reader, 2, spec, "H.265")?;
        }
    }
    for (profile_present, level_present) in sub_layer_profile_present_flags
        .into_iter()
        .zip(sub_layer_level_present_flags)
    {
        if profile_present {
            skip_bits_labeled(&mut reader, 88, spec, "H.265")?;
        }
        if level_present {
            skip_bits_labeled(&mut reader, 8, spec, "H.265")?;
        }
    }

    let _sps_seq_parameter_set_id = read_ue_labeled(&mut reader, spec, "H.265")?;
    let chroma_format_idc =
        u8::try_from(read_ue_labeled(&mut reader, spec, "H.265")?).map_err(|_| {
            MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "H.265 chroma format does not fit in u8".to_string(),
            }
        })?;
    let separate_colour_plane_flag = if chroma_format_idc == 3 {
        read_bit_labeled(&mut reader, spec, "H.265")?
    } else {
        false
    };
    let pic_width_in_luma_samples = read_ue_labeled(&mut reader, spec, "H.265")?;
    let pic_height_in_luma_samples = read_ue_labeled(&mut reader, spec, "H.265")?;
    let (conf_win_left_offset, conf_win_right_offset, conf_win_top_offset, conf_win_bottom_offset) =
        if read_bit_labeled(&mut reader, spec, "H.265")? {
            (
                read_ue_labeled(&mut reader, spec, "H.265")?,
                read_ue_labeled(&mut reader, spec, "H.265")?,
                read_ue_labeled(&mut reader, spec, "H.265")?,
                read_ue_labeled(&mut reader, spec, "H.265")?,
            )
        } else {
            (0, 0, 0, 0)
        };
    let bit_depth_luma_minus8 = u8::try_from(read_ue_labeled(&mut reader, spec, "H.265")?)
        .map_err(|_| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 luma bit depth does not fit in u8".to_string(),
        })?;
    let bit_depth_chroma_minus8 = u8::try_from(read_ue_labeled(&mut reader, spec, "H.265")?)
        .map_err(|_| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 chroma bit depth does not fit in u8".to_string(),
        })?;
    let log2_max_pic_order_cnt_lsb_minus4 = read_ue_labeled(&mut reader, spec, "H.265")?;
    let sps_sub_layer_ordering_info_present_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let sub_layer_ordering_start = if sps_sub_layer_ordering_info_present_flag {
        0
    } else {
        u32::from(max_sub_layers_minus1)
    };
    for _ in sub_layer_ordering_start..=u32::from(max_sub_layers_minus1) {
        let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
        let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
        let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    }
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
    let scaling_list_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    if scaling_list_enabled_flag && read_bit_labeled(&mut reader, spec, "H.265")? {
        skip_h265_scaling_list_data(&mut reader, spec)?;
    }
    let _amp_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let _sample_adaptive_offset_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let pcm_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    if pcm_enabled_flag {
        skip_bits_labeled(&mut reader, 4, spec, "H.265")?;
        skip_bits_labeled(&mut reader, 4, spec, "H.265")?;
        let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
        let _ = read_ue_labeled(&mut reader, spec, "H.265")?;
        let _pcm_loop_filter_disabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    }
    let num_short_term_ref_pic_sets = read_ue_labeled(&mut reader, spec, "H.265")?;
    let mut num_delta_pocs = Vec::with_capacity(
        usize::try_from(num_short_term_ref_pic_sets)
            .map_err(|_| MuxError::LayoutOverflow("H.265 short-term reference picture sets"))?,
    );
    for st_rps_idx in 0..num_short_term_ref_pic_sets {
        skip_h265_short_term_ref_pic_set(
            &mut reader,
            st_rps_idx,
            num_short_term_ref_pic_sets,
            &mut num_delta_pocs,
            spec,
        )?;
    }
    if read_bit_labeled(&mut reader, spec, "H.265")? {
        let num_long_term_ref_pics_sps = read_ue_labeled(&mut reader, spec, "H.265")?;
        let lt_ref_pic_bits =
            usize::try_from(log2_max_pic_order_cnt_lsb_minus4.checked_add(4).ok_or(
                MuxError::LayoutOverflow("H.265 long-term reference picture width"),
            )?)
            .map_err(|_| MuxError::LayoutOverflow("H.265 long-term reference picture width"))?;
        for _ in 0..num_long_term_ref_pics_sps {
            skip_bits_labeled(&mut reader, lt_ref_pic_bits, spec, "H.265")?;
            let _used_by_curr_pic_lt_sps_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
        }
    }
    let _sps_temporal_mvp_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let _strong_intra_smoothing_enabled_flag = read_bit_labeled(&mut reader, spec, "H.265")?;
    let (
        timing_num_units_in_tick,
        timing_time_scale,
        timing_ticks_per_picture,
        pixel_aspect_ratio,
        color_info,
    ) = if read_bit_labeled(&mut reader, spec, "H.265")? {
        parse_h265_vui_timing(&mut reader, spec)?
    } else {
        (None, None, None, None, None)
    };

    let sub_width_c = match chroma_format_idc {
        1 | 2 => 2_u32,
        _ => 1_u32,
    };
    let sub_height_c = match chroma_format_idc {
        1 => 2_u32,
        _ => 1_u32,
    };
    let width = pic_width_in_luma_samples
        .saturating_sub((conf_win_left_offset + conf_win_right_offset).saturating_mul(sub_width_c));
    let height = pic_height_in_luma_samples.saturating_sub(
        (conf_win_top_offset + conf_win_bottom_offset).saturating_mul(sub_height_c),
    );

    Ok(H265SpsInfo {
        width: u16::try_from(width).map_err(|_| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 SPS width does not fit in u16".to_string(),
        })?,
        height: u16::try_from(height).map_err(|_| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "H.265 SPS height does not fit in u16".to_string(),
        })?,
        general_profile_space,
        general_tier_flag,
        general_profile_idc,
        general_profile_compatibility,
        general_constraint_indicator,
        general_level_idc,
        chroma_format_idc,
        separate_colour_plane_flag,
        bit_depth_luma_minus8,
        bit_depth_chroma_minus8,
        num_temporal_layers: max_sub_layers_minus1.saturating_add(1),
        temporal_id_nested,
        log2_max_pic_order_cnt_lsb: u8::try_from(log2_max_pic_order_cnt_lsb_minus4 + 4).map_err(
            |_| MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "H.265 POC width does not fit in u8".to_string(),
            },
        )?,
        timing_time_scale,
        timing_num_units_in_tick,
        timing_ticks_per_picture,
        pixel_aspect_ratio,
        color_info,
    })
}

fn skip_h265_scaling_list_data<R>(reader: &mut BitReader<R>, spec: &str) -> Result<(), MuxError>
where
    R: Read,
{
    for size_id in 0..4 {
        let matrix_count = if size_id == 3 { 2 } else { 6 };
        for _ in 0..matrix_count {
            if !read_bit_labeled(reader, spec, "H.265")? {
                let _ = read_ue_labeled(reader, spec, "H.265")?;
                continue;
            }
            let coef_num = 64_usize.min(1_usize << (4 + (size_id * 2)));
            if size_id > 1 {
                let _ = read_se_labeled(reader, spec, "H.265")?;
            }
            for _ in 0..coef_num {
                let _ = read_se_labeled(reader, spec, "H.265")?;
            }
        }
    }
    Ok(())
}

fn skip_h265_short_term_ref_pic_set<R>(
    reader: &mut BitReader<R>,
    st_rps_idx: u32,
    num_short_term_ref_pic_sets: u32,
    num_delta_pocs: &mut Vec<u32>,
    spec: &str,
) -> Result<(), MuxError>
where
    R: Read,
{
    let inter_ref_pic_set_prediction_flag = if st_rps_idx != 0 {
        read_bit_labeled(reader, spec, "H.265")?
    } else {
        false
    };
    if inter_ref_pic_set_prediction_flag {
        let delta_idx_minus1 = if st_rps_idx == num_short_term_ref_pic_sets {
            read_ue_labeled(reader, spec, "H.265")?
        } else {
            0
        };
        let ref_rps_idx = st_rps_idx
            .checked_sub(delta_idx_minus1 + 1)
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "H.265 short-term reference picture sets underflowed".to_string(),
            })?;
        let ref_num_delta_pocs = *num_delta_pocs
            .get(usize::try_from(ref_rps_idx).map_err(|_| {
                MuxError::LayoutOverflow("H.265 short-term reference picture set index")
            })?)
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "H.265 short-term reference picture sets referenced an unknown set"
                    .to_string(),
            })?;
        let _delta_rps_sign = read_bit_labeled(reader, spec, "H.265")?;
        let _abs_delta_rps_minus1 = read_ue_labeled(reader, spec, "H.265")?;
        let mut resolved_delta_pocs = 0_u32;
        for _ in 0..=ref_num_delta_pocs {
            let used_by_curr_pic_flag = read_bit_labeled(reader, spec, "H.265")?;
            let use_delta_flag = if !used_by_curr_pic_flag {
                read_bit_labeled(reader, spec, "H.265")?
            } else {
                false
            };
            if used_by_curr_pic_flag || use_delta_flag {
                resolved_delta_pocs =
                    resolved_delta_pocs
                        .checked_add(1)
                        .ok_or(MuxError::LayoutOverflow(
                            "H.265 short-term reference picture delta count",
                        ))?;
            }
        }
        num_delta_pocs.push(resolved_delta_pocs);
    } else {
        let num_negative_pics = read_ue_labeled(reader, spec, "H.265")?;
        let num_positive_pics = read_ue_labeled(reader, spec, "H.265")?;
        for _ in 0..num_negative_pics {
            let _ = read_ue_labeled(reader, spec, "H.265")?;
            let _used_by_curr_pic_s0_flag = read_bit_labeled(reader, spec, "H.265")?;
        }
        for _ in 0..num_positive_pics {
            let _ = read_ue_labeled(reader, spec, "H.265")?;
            let _used_by_curr_pic_s1_flag = read_bit_labeled(reader, spec, "H.265")?;
        }
        num_delta_pocs.push(num_negative_pics.checked_add(num_positive_pics).ok_or(
            MuxError::LayoutOverflow("H.265 short-term reference picture count"),
        )?);
    }
    Ok(())
}

fn parse_h265_vui_timing<R>(reader: &mut BitReader<R>, spec: &str) -> Result<H265VuiInfo, MuxError>
where
    R: Read,
{
    let mut pixel_aspect_ratio = None;
    if read_bit_labeled(reader, spec, "H.265")? {
        let aspect_ratio_idc = read_bits_u8_labeled(reader, 8, spec, "H.265")?;
        if aspect_ratio_idc == 255 {
            let sar_width = read_bits_u16_labeled(reader, 16, spec, "H.265")?;
            let sar_height = read_bits_u16_labeled(reader, 16, spec, "H.265")?;
            if sar_width != 0 && sar_height != 0 && sar_width != sar_height {
                pixel_aspect_ratio = Some(H265PixelAspectRatio {
                    h_spacing: u32::from(sar_width),
                    v_spacing: u32::from(sar_height),
                });
            }
        } else {
            pixel_aspect_ratio = h265_pixel_aspect_ratio_from_idc(aspect_ratio_idc);
        }
    }
    if read_bit_labeled(reader, spec, "H.265")? {
        let _overscan_appropriate_flag = read_bit_labeled(reader, spec, "H.265")?;
    }
    let mut color_info = None;
    if read_bit_labeled(reader, spec, "H.265")? {
        let _video_format = read_bits_u8_labeled(reader, 3, spec, "H.265")?;
        let video_full_range_flag = read_bit_labeled(reader, spec, "H.265")?;
        if read_bit_labeled(reader, spec, "H.265")? {
            color_info = Some(H265ColorInfo {
                colour_primaries: u16::from(read_bits_u8_labeled(reader, 8, spec, "H.265")?),
                transfer_characteristics: u16::from(read_bits_u8_labeled(
                    reader, 8, spec, "H.265",
                )?),
                matrix_coefficients: u16::from(read_bits_u8_labeled(reader, 8, spec, "H.265")?),
                full_range_flag: video_full_range_flag,
            });
        }
    }
    if read_bit_labeled(reader, spec, "H.265")? {
        let _ = read_ue_labeled(reader, spec, "H.265")?;
        let _ = read_ue_labeled(reader, spec, "H.265")?;
    }
    let _neutral_chroma_indication_flag = read_bit_labeled(reader, spec, "H.265")?;
    let _field_seq_flag = read_bit_labeled(reader, spec, "H.265")?;
    let _frame_field_info_present_flag = read_bit_labeled(reader, spec, "H.265")?;
    if read_bit_labeled(reader, spec, "H.265")? {
        let _ = read_ue_labeled(reader, spec, "H.265")?;
        let _ = read_ue_labeled(reader, spec, "H.265")?;
        let _ = read_ue_labeled(reader, spec, "H.265")?;
        let _ = read_ue_labeled(reader, spec, "H.265")?;
    }
    if !read_bit_labeled(reader, spec, "H.265")? {
        return Ok((None, None, None, pixel_aspect_ratio, color_info));
    }
    let num_units_in_tick = read_bits_u32_labeled(reader, 32, spec, "H.265")?;
    let time_scale = read_bits_u32_labeled(reader, 32, spec, "H.265")?;
    let ticks_per_picture = if read_bit_labeled(reader, spec, "H.265")? {
        Some(
            read_ue_labeled(reader, spec, "H.265")?
                .checked_add(1)
                .ok_or(MuxError::LayoutOverflow(
                    "H.265 ticks-per-picture timing from VUI",
                ))?,
        )
    } else {
        None
    };
    Ok((
        (num_units_in_tick != 0).then_some(num_units_in_tick),
        (time_scale != 0).then_some(time_scale),
        ticks_per_picture,
        pixel_aspect_ratio,
        color_info,
    ))
}

fn h265_pixel_aspect_ratio_from_idc(aspect_ratio_idc: u8) -> Option<H265PixelAspectRatio> {
    let (h_spacing, v_spacing) = match aspect_ratio_idc {
        1 => (1, 1),
        2 => (12, 11),
        3 => (10, 11),
        4 => (16, 11),
        5 => (40, 33),
        6 => (24, 11),
        7 => (20, 11),
        8 => (32, 11),
        9 => (80, 33),
        10 => (18, 11),
        11 => (15, 11),
        12 => (64, 33),
        13 => (160, 99),
        14 => (4, 3),
        15 => (3, 2),
        16 => (2, 1),
        _ => return None,
    };
    (h_spacing != v_spacing).then_some(H265PixelAspectRatio {
        h_spacing,
        v_spacing,
    })
}
