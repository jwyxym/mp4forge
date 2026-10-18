use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

#[cfg(feature = "async")]
use tokio::fs::File as TokioFile;

use super::super::MuxError;
use super::super::MuxTrackKind;
#[cfg(feature = "async")]
use super::super::import::read_exact_at_async;
use super::super::import::{
    CandidateSample, CompositeTrackCandidate, SegmentedMuxSourceSegment, SegmentedMuxSourceSpec,
    StagedSample, TrackCandidate, build_btrt_from_sample_sizes,
    build_generic_media_sample_entry_box, direct_ingest_handler_name, direct_ingest_mux_policy,
    read_exact_at_sync, with_force_empty_sync_sample_table,
};
#[cfg(feature = "async")]
use super::aac::scan_adts_segmented_async;
use super::aac::scan_adts_segmented_sync;
#[cfg(feature = "async")]
use super::ac3::scan_ac3_segmented_async;
use super::ac3::{build_ac3_sample_entry_box_with_btrt, scan_ac3_segmented_sync};
#[cfg(feature = "async")]
use super::ac4::scan_ac4_segmented_async;
use super::ac4::scan_ac4_segmented_sync;
#[cfg(feature = "async")]
use super::av1::scan_transport_av1_segmented_async;
use super::av1::scan_transport_av1_segmented_sync;
#[cfg(feature = "async")]
use super::avs3::scan_transport_avs3_segmented_async;
use super::avs3::scan_transport_avs3_segmented_sync;
#[cfg(feature = "async")]
use super::container_common::read_segmented_bytes_async;
use super::container_common::{append_file_range_segment, read_segmented_bytes_sync};
#[cfg(feature = "async")]
use super::dts::scan_dts_segmented_async;
use super::dts::{retune_carried_dts_sample_entry_box, scan_dts_segmented_sync};
#[cfg(feature = "async")]
use super::eac3::scan_eac3_segmented_async;
use super::eac3::{build_eac3_sample_entry_box_with_btrt, scan_eac3_segmented_sync};
#[cfg(feature = "async")]
use super::h264::stage_annex_b_h264_segmented_async;
use super::h264::{
    authored_h264_media_duration,
    build_h264_sample_entry_from_avc_config_with_box_type_and_options,
    retune_carried_h264_sample_entry_box, stage_annex_b_h264_segmented_sync,
};
#[cfg(feature = "async")]
use super::h265::stage_annex_b_h265_segmented_async;
use super::h265::stage_annex_b_h265_segmented_sync;
#[cfg(feature = "async")]
use super::latm::scan_latm_segmented_async;
use super::latm::scan_latm_segmented_sync;
#[cfg(feature = "async")]
use super::mhas::scan_mhas_segmented_async;
use super::mhas::{build_mhas_sample_entry_box_with_btrt, scan_mhas_segmented_sync};
use super::mp3::{build_mp3_sample_entry_box, parse_mp3_frame_header};
#[cfg(feature = "async")]
use super::mp4v::scan_mp4v_segmented_async;
use super::mp4v::{
    build_direct_mp4v_sample_entry_box_with_total_duration, scan_mp4v_segmented_sync,
};
#[cfg(feature = "async")]
use super::mpeg2v::scan_mpeg2v_segmented_async;
use super::mpeg2v::{build_transport_mpeg2v_sample_entry_box, scan_mpeg2v_segmented_sync};
#[cfg(feature = "async")]
use super::truehd::scan_truehd_segmented_async;
use super::truehd::{build_truehd_sample_entry_box_with_btrt, scan_truehd_segmented_sync};
#[cfg(feature = "async")]
use super::vvc::stage_annex_b_vvc_segmented_async;
use super::vvc::stage_annex_b_vvc_segmented_sync;
use crate::FourCc;
use crate::boxes::iso14496_12::{AVCDecoderConfiguration, DvsC};

const TS_PACKET_SIZE: usize = 188;
const PAT_PID: u16 = 0x0000;
const STREAM_TYPE_MPEG1_AUDIO: u8 = 0x03;
const STREAM_TYPE_MPEG2_AUDIO: u8 = 0x04;
const STREAM_TYPE_PRIVATE_DATA: u8 = 0x06;
const STREAM_TYPE_AAC_AUDIO: u8 = 0x0F;
const STREAM_TYPE_MPEG4_VIDEO: u8 = 0x10;
const STREAM_TYPE_LATM_AUDIO: u8 = 0x11;
const STREAM_TYPE_MHAS_MAIN: u8 = 0x2D;
const STREAM_TYPE_MHAS_AUX: u8 = 0x2E;
const STREAM_TYPE_H264_VIDEO: u8 = 0x1B;
const STREAM_TYPE_H265_VIDEO: u8 = 0x24;
const STREAM_TYPE_VVC_VIDEO: u8 = 0x33;
const STREAM_TYPE_VVC_VIDEO_TEMPORAL: u8 = 0x34;
const STREAM_TYPE_AC3_AUDIO: u8 = 0x81;
const STREAM_TYPE_DTS_AUDIO: u8 = 0x82;
const STREAM_TYPE_TRUEHD_AUDIO: u8 = 0x83;
const STREAM_TYPE_EAC3_AUDIO: u8 = 0x84;
const STREAM_TYPE_AVS3_VIDEO: u8 = 0xD4;
const PMT_DESCRIPTOR_DVB_TELETEXT: u8 = 0x56;
const PMT_DESCRIPTOR_DVB_SUBTITLE: u8 = 0x59;
const PMT_DESCRIPTOR_PRIVATE_DATA_SPECIFIER: u8 = 0x5F;
const PMT_DESCRIPTOR_REGISTRATION: u8 = 0x05;
const PMT_DESCRIPTOR_AV1_VIDEO: u8 = 0x80;
const PES_STREAM_ID_PRIVATE_STREAM_1: u8 = 0xBD;
const DIRECT_SUBTITLE_TIMESCALE: u32 = 1_000;
const DIRECT_SUBTITLE_SAMPLE_DURATION: u32 = 1_000;
const TRANSPORT_VIDEO_TIMESCALE: u32 = 90_000;
const TRANSPORT_AC3_ANCHOR_JITTER_TOLERANCE_90K: u32 = 16;
const TRANSPORT_INITIAL_PCR_WRAP_BACKOFF_27M: u64 = 4_800;
const TRANSPORT_FLAT_AUDIO_INTERLEAVE_TARGET_90K: u64 = 45_000;
const TRANSPORT_MP4V_FALLBACK_SAMPLE_DURATION: u32 = 3_000;
const REGISTRATION_AVSV: [u8; 4] = *b"AVSV";
const REGISTRATION_AV01: [u8; 4] = *b"AV01";
const REGISTRATION_AC4: [u8; 4] = *b"AC-4";
const REGISTRATION_DTS1: [u8; 4] = *b"DTS1";
const REGISTRATION_DTS2: [u8; 4] = *b"DTS2";
const REGISTRATION_DTS3: [u8; 4] = *b"DTS3";
const PRIVATE_DATA_SPECIFIER_AOMS: [u8; 4] = *b"AOMS";
const TRANSPORT_MAX_PCR_27M: u64 = 2_576_980_377_811;
const TRANSPORT_MAX_PCR_90K: u64 = 8_589_934_592;

#[derive(Clone, Copy)]
enum TransportTrackKind {
    Mp3,
    Aac,
    Latm,
    Mhas,
    Ac3,
    Truehd,
    Eac3,
    Ac4,
    Dts,
    Mpeg2v,
    Av1,
    Avs3,
    Mp4v,
    H264,
    H265,
    Vvc,
    DvbSubtitle,
    DvbTeletext,
}

#[derive(Clone, Copy)]
struct DvbSubtitleConfig {
    language: [u8; 3],
    composition_page_id: u16,
    ancillary_page_id: u16,
    subtitle_type: u8,
}

struct TransportTrackBuilder {
    pid: u16,
    kind: TransportTrackKind,
    segments: Vec<SegmentedMuxSourceSegment>,
    total_size: u64,
    sample_offsets: Vec<u64>,
    pts_anchors: Vec<TransportTimestampAnchor>,
    language: [u8; 3],
    dvb_subtitle: Option<DvbSubtitleConfig>,
    av1_descriptor: Option<[u8; 4]>,
    avs3_config: Option<Vec<u8>>,
}

#[derive(Clone, Copy)]
struct TransportTimestampAnchor {
    sample_offset: u64,
    pts_90k: u64,
}

struct TransportProgramClockState {
    pcr_pid: Option<u16>,
    before_last_pcr_value: Option<u64>,
    last_pcr_value: Option<u64>,
    pcr_base_offset_27m: i64,
    last_continuity_counter: Option<u8>,
}

pub(in crate::mux) struct TransportStreamScanResult {
    pub(in crate::mux) composite_tracks: Vec<CompositeTrackCandidate>,
    pub(in crate::mux) flat_chunk_sample_counts_by_track_id: BTreeMap<u32, Vec<u32>>,
}

struct FinalizedTransportTrack {
    composite_track: CompositeTrackCandidate,
    flat_chunk_sample_counts: Option<Vec<u32>>,
}

impl FinalizedTransportTrack {
    fn without_flat_chunk_sample_counts(composite_track: CompositeTrackCandidate) -> Self {
        Self {
            composite_track,
            flat_chunk_sample_counts: None,
        }
    }
}

struct ParsedTransportPesHeader {
    payload_offset: usize,
    pts_90k: Option<u64>,
}

fn new_transport_track_builder(pid: u16, kind: TransportTrackKind) -> TransportTrackBuilder {
    TransportTrackBuilder {
        pid,
        kind,
        segments: Vec::new(),
        total_size: 0,
        sample_offsets: Vec::new(),
        pts_anchors: Vec::new(),
        language: *b"und",
        dvb_subtitle: None,
        av1_descriptor: None,
        avs3_config: None,
    }
}

fn translate_transport_timestamp_90k(
    state: &TransportProgramClockState,
    pts_90k: u64,
) -> Result<u64, MuxError> {
    let mut translated = i128::from(pts_90k);
    if let Some(last_pcr_value) = state.last_pcr_value {
        if last_pcr_value > (9 * TRANSPORT_MAX_PCR_27M) / 10 && pts_90k < TRANSPORT_MAX_PCR_90K / 10
        {
            translated = translated
                .checked_add(i128::from(TRANSPORT_MAX_PCR_90K))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream timestamp loop translation",
                ))?;
        }
        if TRANSPORT_MAX_PCR_90K < 20_000 + pts_90k && last_pcr_value < 27_000_000 {
            translated = translated
                .checked_add(i128::from(state.pcr_base_offset_27m / 300))
                .and_then(|value| value.checked_sub(i128::from(TRANSPORT_MAX_PCR_90K)))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream timestamp translated offset",
                ))?;
        } else {
            translated = translated
                .checked_add(i128::from(state.pcr_base_offset_27m / 300))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream timestamp translated offset",
                ))?;
        }
    }
    if translated < 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: "transport stream".to_string(),
            message: "translated transport timestamp became negative".to_string(),
        });
    }
    u64::try_from(translated)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream timestamp translation"))
}

fn update_transport_program_clock(
    state: &mut TransportProgramClockState,
    pid: u16,
    adaptation_control: u8,
    continuity_counter: u8,
    discontinuity_signaled: bool,
    pcr_27m: u64,
) {
    if state.pcr_pid != Some(pid) {
        return;
    }
    let continuity_break = match (state.last_continuity_counter, state.before_last_pcr_value) {
        (Some(previous_cc), Some(_)) if adaptation_control == 0x02 => {
            continuity_counter != previous_cc
        }
        (Some(previous_cc), Some(_)) => continuity_counter != ((previous_cc + 1) & 0x0F),
        _ => false,
    };
    state.last_continuity_counter = Some(continuity_counter);

    let prev_diff_27m = match (state.before_last_pcr_value, state.last_pcr_value) {
        (Some(before_last), Some(last)) => i64::try_from(last)
            .ok()
            .and_then(|last| {
                i64::try_from(before_last)
                    .ok()
                    .map(|before_last| last - before_last)
            })
            .unwrap_or(0),
        _ => 0,
    };
    let previous_last_pcr = state.last_pcr_value;
    state.before_last_pcr_value = previous_last_pcr;
    state.last_pcr_value = Some(pcr_27m.max(1));

    let Some(before_last_pcr) = previous_last_pcr else {
        if pcr_27m > (9 * TRANSPORT_MAX_PCR_27M) / 10 {
            let synthetic_previous = if TRANSPORT_MAX_PCR_27M.saturating_sub(pcr_27m)
                > TRANSPORT_INITIAL_PCR_WRAP_BACKOFF_27M
            {
                TRANSPORT_MAX_PCR_27M - TRANSPORT_INITIAL_PCR_WRAP_BACKOFF_27M
            } else {
                TRANSPORT_MAX_PCR_27M
            };
            state.before_last_pcr_value = Some(synthetic_previous);
        }
        return;
    };
    let diff_27m = i64::try_from(pcr_27m)
        .ok()
        .and_then(|current| {
            i64::try_from(before_last_pcr)
                .ok()
                .map(|previous| current - previous)
        })
        .unwrap_or(0);
    let prev_diff_in_us = prev_diff_27m / 27;
    let diff_in_us = diff_27m / 27;
    let mut adjust_pcr = false;

    if discontinuity_signaled || continuity_break {
        let delta_from_previous = (diff_in_us - prev_diff_in_us).abs();
        if (-200_000..0).contains(&diff_in_us) || (diff_in_us > 0 && delta_from_previous < 200_000)
        {
        } else {
            adjust_pcr = true;
        }
    } else if diff_27m.unsigned_abs() > 270_000_000_u64 {
        if pcr_27m < before_last_pcr && (TRANSPORT_MAX_PCR_27M - before_last_pcr) < 5_400_000 {
            state.pcr_base_offset_27m = state
                .pcr_base_offset_27m
                .saturating_add(i64::try_from(TRANSPORT_MAX_PCR_27M).unwrap());
            return;
        }
        if (-200_000..0).contains(&diff_in_us) || pcr_27m < before_last_pcr {
            adjust_pcr = true;
        }
    }

    if adjust_pcr {
        let expected_next_pcr = i128::from(before_last_pcr)
            .checked_add(i128::from(prev_diff_in_us) * 27)
            .unwrap_or(i128::from(before_last_pcr));
        let delta = expected_next_pcr - i128::from(pcr_27m);
        state.pcr_base_offset_27m =
            state
                .pcr_base_offset_27m
                .saturating_add(i64::try_from(delta).unwrap_or_else(|_| {
                    if delta.is_negative() {
                        i64::MIN
                    } else {
                        i64::MAX
                    }
                }));
    }
}

fn transport_track_uses_full_au(kind: TransportTrackKind) -> bool {
    matches!(
        kind,
        TransportTrackKind::Av1
            | TransportTrackKind::Avs3
            | TransportTrackKind::DvbSubtitle
            | TransportTrackKind::DvbTeletext
    )
}

fn transport_track_uses_program_clock_translation(kind: TransportTrackKind) -> bool {
    matches!(
        kind,
        TransportTrackKind::Mp3
            | TransportTrackKind::Aac
            | TransportTrackKind::Latm
            | TransportTrackKind::Mhas
            | TransportTrackKind::Ac3
            | TransportTrackKind::Truehd
            | TransportTrackKind::Eac3
            | TransportTrackKind::Ac4
            | TransportTrackKind::Dts
    )
}

pub(in crate::mux) fn scan_transport_stream_sync(
    path: &Path,
    spec: &str,
) -> Result<TransportStreamScanResult, MuxError> {
    let mut file = File::open(path)?;
    let file_size = file.metadata()?.len();
    validate_transport_stream_sync(&mut file, file_size, spec)?;

    let mut pmt_pid = None::<u16>;
    let mut builders = BTreeMap::<u16, TransportTrackBuilder>::new();
    let mut program_clock = TransportProgramClockState {
        pcr_pid: None,
        before_last_pcr_value: None,
        last_pcr_value: None,
        pcr_base_offset_27m: 0,
        last_continuity_counter: None,
    };
    let mut offset = 0_u64;
    while offset + u64::try_from(TS_PACKET_SIZE).unwrap() <= file_size {
        let mut packet = [0_u8; TS_PACKET_SIZE];
        read_exact_at_sync(
            &mut file,
            offset,
            &mut packet,
            spec,
            "truncated MPEG transport stream packet",
        )?;
        parse_transport_packet_sync(
            spec,
            &packet,
            offset,
            &mut pmt_pid,
            &mut builders,
            &mut program_clock,
        )?;
        offset += u64::try_from(TS_PACKET_SIZE).unwrap();
    }

    finalize_transport_tracks_sync(path, spec, &mut file, builders)
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn scan_transport_stream_async(
    path: &Path,
    spec: &str,
) -> Result<TransportStreamScanResult, MuxError> {
    let mut file = TokioFile::open(path).await?;
    let file_size = file.metadata().await?.len();
    validate_transport_stream_async(&mut file, file_size, spec).await?;

    let mut pmt_pid = None::<u16>;
    let mut builders = BTreeMap::<u16, TransportTrackBuilder>::new();
    let mut program_clock = TransportProgramClockState {
        pcr_pid: None,
        before_last_pcr_value: None,
        last_pcr_value: None,
        pcr_base_offset_27m: 0,
        last_continuity_counter: None,
    };
    let mut offset = 0_u64;
	// Tokio file operations use blocking tasks internally. Read many complete
	// packets at once instead of scheduling a seek/read pair for every 188 B.
	const PACKETS_PER_READ: usize = 4096;
	let mut buffer: Vec<u8> = vec![0; TS_PACKET_SIZE * PACKETS_PER_READ];
	while file_size - offset >= TS_PACKET_SIZE as u64 {
		let packet_count: usize = ((file_size - offset) / TS_PACKET_SIZE as u64)
			.min(PACKETS_PER_READ as u64) as usize;
		let length: usize = packet_count * TS_PACKET_SIZE;
		read_exact_at_async(&mut file, offset, &mut buffer[..length], spec,
			"truncated MPEG transport stream packet").await?;
		for (index, bytes) in buffer[..length].chunks_exact(TS_PACKET_SIZE).enumerate() {
			let packet: &[u8; TS_PACKET_SIZE] = bytes.try_into()
				.map_err(|_| MuxError::LayoutOverflow("transport-stream packet size"))?;
			let packet_offset: u64 = offset + (index * TS_PACKET_SIZE) as u64;
			parse_transport_packet_sync(spec, packet, packet_offset, &mut pmt_pid,
				&mut builders, &mut program_clock)?;
		}
		offset += length as u64;
	}

    finalize_transport_tracks_async(path, spec, &mut file, builders).await
}

fn validate_transport_stream_sync(
    file: &mut File,
    file_size: u64,
    spec: &str,
) -> Result<(), MuxError> {
    if file_size < u64::try_from(TS_PACKET_SIZE * 2).unwrap() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport stream input is too short to validate packet sync".to_string(),
        });
    }
    let mut prefix = [0_u8; TS_PACKET_SIZE * 2];
    read_exact_at_sync(
        file,
        0,
        &mut prefix,
        spec,
        "transport stream input is truncated before the first two packets",
    )?;
    if prefix[0] != 0x47 || prefix[TS_PACKET_SIZE] != 0x47 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "input does not carry MPEG transport stream packet sync bytes".to_string(),
        });
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn validate_transport_stream_async(
    file: &mut TokioFile,
    file_size: u64,
    spec: &str,
) -> Result<(), MuxError> {
    if file_size < u64::try_from(TS_PACKET_SIZE * 2).unwrap() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport stream input is too short to validate packet sync".to_string(),
        });
    }
    let mut prefix = [0_u8; TS_PACKET_SIZE * 2];
    read_exact_at_async(
        file,
        0,
        &mut prefix,
        spec,
        "transport stream input is truncated before the first two packets",
    )
    .await?;
    if prefix[0] != 0x47 || prefix[TS_PACKET_SIZE] != 0x47 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "input does not carry MPEG transport stream packet sync bytes".to_string(),
        });
    }
    Ok(())
}

fn parse_transport_packet_sync(
    spec: &str,
    packet: &[u8; TS_PACKET_SIZE],
    packet_offset: u64,
    pmt_pid: &mut Option<u16>,
    builders: &mut BTreeMap<u16, TransportTrackBuilder>,
    program_clock: &mut TransportProgramClockState,
) -> Result<(), MuxError> {
    if packet[0] != 0x47 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!("missing transport-stream sync byte at packet offset {packet_offset}"),
        });
    }
    if packet[1] & 0x80 != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream packets with transport errors are not supported".to_string(),
        });
    }
    if packet[3] & 0xC0 != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "scrambled transport-stream packets are not supported".to_string(),
        });
    }
    let payload_unit_start = packet[1] & 0x40 != 0;
    let pid = (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2]);
    let adaptation_control = (packet[3] >> 4) & 0x03;
    if adaptation_control == 0x00 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream packets with reserved adaptation-control state are not supported"
                    .to_string(),
        });
    }
    if adaptation_control == 0x02 {
        if let Some((discontinuity_signaled, pcr_27m)) = parse_transport_packet_pcr(spec, packet)? {
            update_transport_program_clock(
                program_clock,
                pid,
                adaptation_control,
                packet[3] & 0x0F,
                discontinuity_signaled,
                pcr_27m,
            );
        }
        return Ok(());
    }
    let mut payload_offset = 4usize;
    if adaptation_control == 0x03 {
        if let Some((discontinuity_signaled, pcr_27m)) = parse_transport_packet_pcr(spec, packet)? {
            update_transport_program_clock(
                program_clock,
                pid,
                adaptation_control,
                packet[3] & 0x0F,
                discontinuity_signaled,
                pcr_27m,
            );
        }
        let adaptation_length = usize::from(packet[4]);
        payload_offset =
            payload_offset
                .checked_add(1 + adaptation_length)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream adaptation field",
                ))?;
        if payload_offset > TS_PACKET_SIZE {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream adaptation field overflowed the packet payload"
                    .to_string(),
            });
        }
    }
    if payload_offset >= TS_PACKET_SIZE {
        return Ok(());
    }
    let payload = &packet[payload_offset..];

    if pid == PAT_PID {
        if payload_unit_start && let Some(found_pmt_pid) = parse_pat_section(spec, payload)? {
            *pmt_pid = Some(found_pmt_pid);
        }
        return Ok(());
    }
    if Some(pid) == *pmt_pid {
        if payload_unit_start {
            parse_pmt_section(spec, payload, builders, program_clock)?;
        }
        return Ok(());
    }
    let Some(builder) = builders.get_mut(&pid) else {
        return Ok(());
    };
    if payload_unit_start {
        let pes_header = parse_ts_pes_header(spec, payload, builder.kind)?;
        if let Some(pts_90k) = pes_header.pts_90k {
            builder.pts_anchors.push(TransportTimestampAnchor {
                sample_offset: builder.total_size,
                pts_90k: if transport_track_uses_program_clock_translation(builder.kind) {
                    translate_transport_timestamp_90k(program_clock, pts_90k)?
                } else {
                    pts_90k
                },
            });
        }
        if transport_track_uses_full_au(builder.kind) {
            builder.sample_offsets.push(builder.total_size);
        }
        let pes_payload = &payload[pes_header.payload_offset..];
        if !pes_payload.is_empty() {
            append_file_range_segment(
                &mut builder.segments,
                &mut builder.total_size,
                packet_offset + u64::try_from(payload_offset + pes_header.payload_offset).unwrap(),
                u32::try_from(pes_payload.len())
                    .map_err(|_| MuxError::LayoutOverflow("transport-stream PES payload"))?,
            );
        }
    } else if !payload.is_empty() {
        append_file_range_segment(
            &mut builder.segments,
            &mut builder.total_size,
            packet_offset + u64::try_from(payload_offset).unwrap(),
            u32::try_from(payload.len())
                .map_err(|_| MuxError::LayoutOverflow("transport-stream packet payload"))?,
        );
    }
    Ok(())
}

fn parse_pat_section(spec: &str, payload: &[u8]) -> Result<Option<u16>, MuxError> {
    if payload.is_empty() {
        return Ok(None);
    }
    let pointer_field = usize::from(payload[0]);
    let start = 1usize
        .checked_add(pointer_field)
        .ok_or(MuxError::LayoutOverflow("PAT pointer field"))?;
    if payload.len() < start + 8 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated PAT section".to_string(),
        });
    }
    if payload[start] != 0x00 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "unsupported PAT table id".to_string(),
        });
    }
    let section_length =
        usize::from(u16::from_be_bytes([payload[start + 1], payload[start + 2]]) & 0x0FFF);
    if payload.len() < start + 3 + section_length {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated PAT payload".to_string(),
        });
    }
    validate_transport_section_crc(spec, "PAT", &payload[start..start + 3 + section_length])?;
    let mut entry_offset = start + 8;
    let section_end = start + 3 + section_length - 4;
    let mut found = None::<u16>;
    while entry_offset + 4 <= section_end {
        let program_number = u16::from_be_bytes([payload[entry_offset], payload[entry_offset + 1]]);
        let pid = (u16::from(payload[entry_offset + 2] & 0x1F) << 8)
            | u16::from(payload[entry_offset + 3]);
        if program_number != 0 && found.is_none() {
            found = Some(pid);
        }
        entry_offset += 4;
    }
    Ok(found)
}

fn parse_transport_packet_pcr(
    spec: &str,
    packet: &[u8; TS_PACKET_SIZE],
) -> Result<Option<(bool, u64)>, MuxError> {
    let adaptation_control = (packet[3] >> 4) & 0x03;
    if adaptation_control != 0x02 && adaptation_control != 0x03 {
        return Ok(None);
    }
    let adaptation_length = usize::from(packet[4]);
    if adaptation_length == 0 {
        return Ok(None);
    }
    if 5 + adaptation_length > TS_PACKET_SIZE {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream adaptation field overflowed the packet payload".to_string(),
        });
    }
    let adaptation_flags = packet[5];
    if adaptation_flags & 0x10 == 0 {
        return Ok(None);
    }
    if adaptation_length < 7 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream adaptation field carried a truncated PCR payload"
                .to_string(),
        });
    }
    let pcr_bytes = &packet[6..12];
    let pcr_base = (u64::from(pcr_bytes[0]) << 25)
        | (u64::from(pcr_bytes[1]) << 17)
        | (u64::from(pcr_bytes[2]) << 9)
        | (u64::from(pcr_bytes[3]) << 1)
        | u64::from(pcr_bytes[4] >> 7);
    let pcr_ext = (u16::from(pcr_bytes[4] & 0x01) << 8) | u16::from(pcr_bytes[5]);
    Ok(Some((
        adaptation_flags & 0x80 != 0,
        pcr_base
            .checked_mul(300)
            .and_then(|value| value.checked_add(u64::from(pcr_ext)))
            .ok_or(MuxError::LayoutOverflow("transport-stream PCR timestamp"))?,
    )))
}

fn parse_pmt_section(
    spec: &str,
    payload: &[u8],
    builders: &mut BTreeMap<u16, TransportTrackBuilder>,
    program_clock: &mut TransportProgramClockState,
) -> Result<(), MuxError> {
    if payload.is_empty() {
        return Ok(());
    }
    let pointer_field = usize::from(payload[0]);
    let start = 1usize
        .checked_add(pointer_field)
        .ok_or(MuxError::LayoutOverflow("PMT pointer field"))?;
    if payload.len() < start + 12 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated PMT section".to_string(),
        });
    }
    if payload[start] != 0x02 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "unsupported PMT table id".to_string(),
        });
    }
    let section_length =
        usize::from(u16::from_be_bytes([payload[start + 1], payload[start + 2]]) & 0x0FFF);
    if payload.len() < start + 3 + section_length {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated PMT payload".to_string(),
        });
    }
    validate_transport_section_crc(spec, "PMT", &payload[start..start + 3 + section_length])?;
    let pcr_pid = (u16::from(payload[start + 8] & 0x1F) << 8) | u16::from(payload[start + 9]);
    program_clock.pcr_pid = Some(pcr_pid);
    let program_info_length =
        usize::from(u16::from_be_bytes([payload[start + 10], payload[start + 11]]) & 0x0FFF);
    let mut entry_offset = start + 12 + program_info_length;
    let section_end = start + 3 + section_length - 4;
    while entry_offset + 5 <= section_end {
        let stream_type = payload[entry_offset];
        let elementary_pid = (u16::from(payload[entry_offset + 1] & 0x1F) << 8)
            | u16::from(payload[entry_offset + 2]);
        let es_info_length = usize::from(
            u16::from_be_bytes([payload[entry_offset + 3], payload[entry_offset + 4]]) & 0x0FFF,
        );
        let es_info_start = entry_offset
            .checked_add(5)
            .ok_or(MuxError::LayoutOverflow("PMT elementary-stream info start"))?;
        let es_info_end = es_info_start
            .checked_add(es_info_length)
            .ok_or(MuxError::LayoutOverflow("PMT elementary-stream info end"))?;
        if es_info_end > section_end {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT elementary-stream descriptor payload".to_string(),
            });
        }
        let es_info = &payload[es_info_start..es_info_end];
        match stream_type {
            STREAM_TYPE_MPEG1_AUDIO | STREAM_TYPE_MPEG2_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Mp3)
                });
            }
            STREAM_TYPE_AAC_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Aac)
                });
            }
            STREAM_TYPE_LATM_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Latm)
                });
            }
            STREAM_TYPE_MHAS_MAIN | STREAM_TYPE_MHAS_AUX => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Mhas)
                });
            }
            STREAM_TYPE_AC3_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Ac3)
                });
            }
            STREAM_TYPE_DTS_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Dts)
                });
            }
            STREAM_TYPE_TRUEHD_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Truehd)
                });
            }
            STREAM_TYPE_EAC3_AUDIO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Eac3)
                });
            }
            0x02 => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Mpeg2v)
                });
            }
            STREAM_TYPE_MPEG4_VIDEO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Mp4v)
                });
            }
            STREAM_TYPE_H264_VIDEO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::H264)
                });
            }
            STREAM_TYPE_H265_VIDEO => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::H265)
                });
            }
            STREAM_TYPE_VVC_VIDEO | STREAM_TYPE_VVC_VIDEO_TEMPORAL => {
                builders.entry(elementary_pid).or_insert_with(|| {
                    new_transport_track_builder(elementary_pid, TransportTrackKind::Vvc)
                });
            }
            STREAM_TYPE_PRIVATE_DATA => {
                if let Some(av1_descriptor) = parse_transport_av1_video_descriptor(spec, es_info)? {
                    builders.entry(elementary_pid).or_insert_with(|| {
                        let mut builder =
                            new_transport_track_builder(elementary_pid, TransportTrackKind::Av1);
                        builder.av1_descriptor = Some(av1_descriptor);
                        builder
                    });
                } else if let Some(track) = parse_transport_private_data_track(spec, es_info)? {
                    builders.entry(elementary_pid).or_insert_with(|| {
                        let mut builder = new_transport_track_builder(elementary_pid, track.kind);
                        builder.language = track.language;
                        builder.dvb_subtitle = track.dvb_subtitle;
                        builder
                    });
                }
            }
            STREAM_TYPE_AVS3_VIDEO => {
                let avs3_config = parse_transport_avs3_video_descriptor(spec, es_info)?;
                builders.entry(elementary_pid).or_insert_with(|| {
                    let mut builder =
                        new_transport_track_builder(elementary_pid, TransportTrackKind::Avs3);
                    builder.avs3_config = Some(avs3_config);
                    builder
                });
            }
            other => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message: format!(
                        "transport-stream stream type 0x{other:02X} is not supported on the native direct-ingest path yet"
                    ),
                });
            }
        }
        entry_offset = es_info_end;
    }
    Ok(())
}

fn validate_transport_section_crc(
    spec: &str,
    table_name: &str,
    section: &[u8],
) -> Result<(), MuxError> {
    if section.len() < 4 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!("truncated {table_name} CRC32 field"),
        });
    }
    let crc_offset = section.len() - 4;
    let expected_crc =
        u32::from_be_bytes(section[crc_offset..].try_into().expect("4-byte CRC field"));
    let actual_crc = mpeg2ts_section_crc32(&section[..crc_offset]);
    if actual_crc != expected_crc {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!("{table_name} section failed CRC32 validation"),
        });
    }
    Ok(())
}

fn mpeg2ts_section_crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in data {
        crc ^= u32::from(*byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04C1_1DB7
            } else {
                crc << 1
            };
        }
    }
    crc
}

fn parse_transport_av1_video_descriptor(
    spec: &str,
    es_info: &[u8],
) -> Result<Option<[u8; 4]>, MuxError> {
    let mut descriptor_offset = 0usize;
    let mut saw_registration = false;
    let mut saw_private_data_specifier = false;
    let mut av1_descriptor = None::<[u8; 4]>;

    while descriptor_offset < es_info.len() {
        if es_info.len() - descriptor_offset < 2 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor header".to_string(),
            });
        }
        let descriptor_tag = es_info[descriptor_offset];
        let descriptor_length = usize::from(es_info[descriptor_offset + 1]);
        let descriptor_end = descriptor_offset
            .checked_add(2 + descriptor_length)
            .ok_or(MuxError::LayoutOverflow("PMT descriptor length"))?;
        if descriptor_end > es_info.len() {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor payload".to_string(),
            });
        }
        let descriptor_payload = &es_info[descriptor_offset + 2..descriptor_end];
        match descriptor_tag {
            PMT_DESCRIPTOR_REGISTRATION if descriptor_payload == REGISTRATION_AV01 => {
                if saw_registration {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "multiple transport-stream AV1 registration descriptors are not supported on the native direct-ingest path yet"
                                .to_string(),
                    });
                }
                saw_registration = true;
            }
            PMT_DESCRIPTOR_PRIVATE_DATA_SPECIFIER
                if descriptor_payload == PRIVATE_DATA_SPECIFIER_AOMS =>
            {
                if saw_private_data_specifier {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "multiple transport-stream AV1 private-data specifier descriptors are not supported on the native direct-ingest path yet"
                                .to_string(),
                    });
                }
                saw_private_data_specifier = true;
            }
            PMT_DESCRIPTOR_AV1_VIDEO => {
                if descriptor_length != 4 {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "transport-stream AV1 carried descriptor 0x80 with an invalid payload size"
                                .to_string(),
                    });
                }
                if av1_descriptor
                    .replace([
                        descriptor_payload[0],
                        descriptor_payload[1],
                        descriptor_payload[2],
                        descriptor_payload[3],
                    ])
                    .is_some()
                {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "multiple transport-stream AV1 descriptor 0x80 payloads are not supported on the native direct-ingest path yet"
                                .to_string(),
                    });
                }
            }
            _ => {}
        }
        descriptor_offset = descriptor_end;
    }

    match (saw_registration, saw_private_data_specifier, av1_descriptor) {
        (false, false, None) => Ok(None),
        (true, true, Some(descriptor)) => Ok(Some(descriptor)),
        _ => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AV1 private-data carriage must carry AV01 registration, AOMS private-data specifier, and descriptor 0x80 on the native direct-ingest path"
                    .to_string(),
        }),
    }
}

fn parse_transport_avs3_video_descriptor(spec: &str, es_info: &[u8]) -> Result<Vec<u8>, MuxError> {
    let mut descriptor_offset = 0usize;
    let mut found = None::<Vec<u8>>;
    while descriptor_offset < es_info.len() {
        if es_info.len() - descriptor_offset < 2 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor header".to_string(),
            });
        }
        let descriptor_tag = es_info[descriptor_offset];
        let descriptor_length = usize::from(es_info[descriptor_offset + 1]);
        let descriptor_end = descriptor_offset
            .checked_add(2 + descriptor_length)
            .ok_or(MuxError::LayoutOverflow("PMT descriptor length"))?;
        if descriptor_end > es_info.len() {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor payload".to_string(),
            });
        }
        if descriptor_tag == PMT_DESCRIPTOR_REGISTRATION {
            let descriptor_payload = &es_info[descriptor_offset + 2..descriptor_end];
            if descriptor_payload.starts_with(&REGISTRATION_AVSV) {
                if descriptor_payload.len() < 14 {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "transport-stream AVS3 registration descriptor did not carry the full decoder configuration payload"
                                .to_string(),
                    });
                }
                let config = descriptor_payload[4..14].to_vec();
                if found.replace(config).is_some() {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "multiple AVS3 registration descriptors are not supported on the native direct-ingest path yet"
                                .to_string(),
                    });
                }
            }
        }
        descriptor_offset = descriptor_end;
    }
    found.ok_or(MuxError::UnsupportedTrackImport {
        spec: spec.to_string(),
        message:
            "transport-stream AVS3 video carriage is missing its AVSV registration descriptor payload"
                .to_string(),
    })
}

#[derive(Clone, Copy)]
struct TransportPrivateDataTrack {
    kind: TransportTrackKind,
    language: [u8; 3],
    dvb_subtitle: Option<DvbSubtitleConfig>,
}

fn parse_transport_private_data_track(
    spec: &str,
    es_info: &[u8],
) -> Result<Option<TransportPrivateDataTrack>, MuxError> {
    let mut descriptor_offset = 0usize;
    let mut found = None::<TransportPrivateDataTrack>;
    while descriptor_offset < es_info.len() {
        if es_info.len() - descriptor_offset < 2 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor header".to_string(),
            });
        }
        let descriptor_tag = es_info[descriptor_offset];
        let descriptor_length = usize::from(es_info[descriptor_offset + 1]);
        let descriptor_end = descriptor_offset
            .checked_add(2 + descriptor_length)
            .ok_or(MuxError::LayoutOverflow("PMT descriptor length"))?;
        if descriptor_end > es_info.len() {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated PMT descriptor payload".to_string(),
            });
        }
        let descriptor_payload = &es_info[descriptor_offset + 2..descriptor_end];
        let parsed = match descriptor_tag {
            PMT_DESCRIPTOR_REGISTRATION => {
                parse_transport_registration_descriptor(descriptor_payload)
            }
            PMT_DESCRIPTOR_DVB_SUBTITLE => {
                Some(parse_dvb_subtitle_descriptor(spec, descriptor_payload)?)
            }
            PMT_DESCRIPTOR_DVB_TELETEXT => {
                Some(parse_dvb_teletext_descriptor(spec, descriptor_payload)?)
            }
            _ => None,
        };
        if let Some(parsed) = parsed
            && found.replace(parsed).is_some()
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "multiple transport-stream private-data descriptor track declarations are not supported on the native direct-ingest path yet"
                        .to_string(),
            });
        }
        descriptor_offset = descriptor_end;
    }
    Ok(found)
}

fn parse_transport_registration_descriptor(
    descriptor_payload: &[u8],
) -> Option<TransportPrivateDataTrack> {
    let registration = descriptor_payload.get(..4)?;
    if registration == REGISTRATION_DTS1
        || registration == REGISTRATION_DTS2
        || registration == REGISTRATION_DTS3
    {
        return Some(TransportPrivateDataTrack {
            kind: TransportTrackKind::Dts,
            language: *b"und",
            dvb_subtitle: None,
        });
    }
    if registration == REGISTRATION_AC4 {
        return Some(TransportPrivateDataTrack {
            kind: TransportTrackKind::Ac4,
            language: *b"und",
            dvb_subtitle: None,
        });
    }
    None
}

fn parse_dvb_subtitle_descriptor(
    spec: &str,
    descriptor_payload: &[u8],
) -> Result<TransportPrivateDataTrack, MuxError> {
    if descriptor_payload.len() < 8 || !descriptor_payload.len().is_multiple_of(8) {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream DVB subtitle descriptors must contain whole 8-byte service entries".to_string(),
        });
    }
    let entry = &descriptor_payload[..8];
    Ok(TransportPrivateDataTrack {
        kind: TransportTrackKind::DvbSubtitle,
        language: [entry[0], entry[1], entry[2]],
        dvb_subtitle: Some(DvbSubtitleConfig {
            language: [entry[0], entry[1], entry[2]],
            subtitle_type: entry[3],
            composition_page_id: u16::from_be_bytes([entry[4], entry[5]]),
            ancillary_page_id: u16::from_be_bytes([entry[6], entry[7]]),
        }),
    })
}

fn parse_dvb_teletext_descriptor(
    spec: &str,
    descriptor_payload: &[u8],
) -> Result<TransportPrivateDataTrack, MuxError> {
    if descriptor_payload.len() < 5 || !descriptor_payload.len().is_multiple_of(5) {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream DVB teletext descriptors must contain whole 5-byte service entries".to_string(),
        });
    }
    let entry = &descriptor_payload[..5];
    Ok(TransportPrivateDataTrack {
        kind: TransportTrackKind::DvbTeletext,
        language: [entry[0], entry[1], entry[2]],
        dvb_subtitle: None,
    })
}

fn parse_ts_pes_header(
    spec: &str,
    payload: &[u8],
    kind: TransportTrackKind,
) -> Result<ParsedTransportPesHeader, MuxError> {
    if payload.len() < 9 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated transport-stream PES header".to_string(),
        });
    }
    if payload[..3] != [0x00, 0x00, 0x01] {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream payload-unit start did not begin with a PES start code"
                .to_string(),
        });
    }
    match kind {
        TransportTrackKind::Mp3
        | TransportTrackKind::Aac
        | TransportTrackKind::Latm
        | TransportTrackKind::Mhas
            if !(0xC0..=0xDF).contains(&payload[3]) =>
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream PES stream id is not a supported MPEG audio stream"
                    .to_string(),
            });
        }
        TransportTrackKind::Ac3
        | TransportTrackKind::Truehd
        | TransportTrackKind::Eac3
        | TransportTrackKind::Ac4
        | TransportTrackKind::Dts
            if payload[3] != PES_STREAM_ID_PRIVATE_STREAM_1 =>
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream PES stream id is not a supported private audio stream"
                    .to_string(),
            });
        }
        TransportTrackKind::Mpeg2v
        | TransportTrackKind::Av1
        | TransportTrackKind::Avs3
        | TransportTrackKind::Mp4v
            if !(0xE0..=0xEF).contains(&payload[3]) =>
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream PES stream id is not a supported MPEG video stream"
                    .to_string(),
            });
        }
        TransportTrackKind::H264 | TransportTrackKind::H265 | TransportTrackKind::Vvc
            if !(0xE0..=0xEF).contains(&payload[3]) =>
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream PES stream id is not a supported video stream"
                    .to_string(),
            });
        }
        TransportTrackKind::DvbSubtitle | TransportTrackKind::DvbTeletext
            if payload[3] != PES_STREAM_ID_PRIVATE_STREAM_1 =>
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "transport-stream PES stream id is not a supported private subtitle or teletext stream"
                        .to_string(),
            });
        }
        _ => {}
    }
    if payload[6] & 0xC0 != 0x80 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "unsupported transport-stream PES header flags".to_string(),
        });
    }
    let pts_dts_flags = payload[7] & 0xC0;
    let header_data_length = usize::from(payload[8]);
    let payload_offset = 9usize
        .checked_add(header_data_length)
        .ok_or(MuxError::LayoutOverflow("transport-stream PES header"))?;
    if payload_offset > payload.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "truncated transport-stream PES optional header".to_string(),
        });
    }
    let pts_90k = match pts_dts_flags {
        0x00 => None,
        0x80 | 0xC0 => {
            if header_data_length < 5 {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream PES header declared timestamps without enough header bytes"
                            .to_string(),
                });
            }
            Some(parse_ts_pes_timestamp(spec, &payload[9..14])?)
        }
        _ => {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "transport-stream PES header used a reserved PTS/DTS flag state"
                    .to_string(),
            });
        }
    };
    Ok(ParsedTransportPesHeader {
        payload_offset,
        pts_90k,
    })
}

fn parse_ts_pes_timestamp(spec: &str, encoded: &[u8]) -> Result<u64, MuxError> {
    if encoded.len() < 5 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream PES timestamp is truncated".to_string(),
        });
    }
    if encoded[0] & 0x01 == 0 || encoded[2] & 0x01 == 0 || encoded[4] & 0x01 == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream PES timestamp marker bits were invalid".to_string(),
        });
    }
    Ok((u64::from((encoded[0] >> 1) & 0x07) << 30)
        | (u64::from(encoded[1]) << 22)
        | (u64::from((encoded[2] >> 1) & 0x7F) << 15)
        | (u64::from(encoded[3]) << 7)
        | u64::from((encoded[4] >> 1) & 0x7F))
}

fn rescale_transport_audio_time(
    value: i64,
    source_timescale: u32,
    spec: &str,
    label: &str,
) -> Result<i64, MuxError> {
    if source_timescale == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!("{label} used an invalid zero timescale"),
        });
    }

    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream audio time rescale",
        ))?;
    let divisor = u64::from(source_timescale);
    let normalized = scaled
        .checked_add(divisor / 2)
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream audio time rounding",
        ))?
        / divisor;
    let normalized = i64::try_from(normalized)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream audio time rescale"))?;
    Ok(normalized * sign)
}

fn build_transport_timestamped_audio_samples(
    spec: &str,
    label: &str,
    samples: Vec<StagedSample>,
    source_timescale: u32,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<(u32, Vec<CandidateSample>), MuxError> {
    fn rescaled_transport_audio_sample(
        spec: &str,
        label: &str,
        sample: &StagedSample,
        duration: u32,
        source_timescale: u32,
    ) -> Result<CandidateSample, MuxError> {
        Ok(CandidateSample {
            source_index: usize::MAX,
            data_offset: sample.data_offset,
            data_size: sample.data_size,
            duration,
            composition_time_offset: i32::try_from(rescale_transport_audio_time(
                i64::from(sample.composition_time_offset),
                source_timescale,
                spec,
                label,
            )?)
            .map_err(|_| {
                MuxError::LayoutOverflow("transport-stream timestamped audio composition offset")
            })?,
            is_sync_sample: sample.is_sync_sample,
        })
    }

    fn floored_transport_audio_samples(
        spec: &str,
        label: &str,
        samples: &[StagedSample],
        source_timescale: u32,
    ) -> Result<Vec<CandidateSample>, MuxError> {
        samples
            .iter()
            .map(|sample| {
                let duration = u64::from(sample.duration)
                    .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
                    .ok_or(MuxError::LayoutOverflow(
                        "transport-stream timestamped audio duration rescale",
                    ))?
                    / u64::from(source_timescale);
                let duration = u32::try_from(duration).map_err(|_| {
                    MuxError::LayoutOverflow("transport-stream timestamped audio duration")
                })?;
                rescaled_transport_audio_sample(spec, label, sample, duration, source_timescale)
            })
            .collect()
    }

    fn wrapped_transport_audio_anchor_delta(
        current_pts: u64,
        next_pts: u64,
    ) -> Result<u32, MuxError> {
        let delta = (i128::from(next_pts) - i128::from(current_pts)).rem_euclid(1_i128 << 32);
        u32::try_from(delta)
            .map_err(|_| MuxError::LayoutOverflow("transport-stream timestamped audio duration"))
    }

    if pts_anchors.is_empty() {
        return Ok((
            TRANSPORT_VIDEO_TIMESCALE,
            floored_transport_audio_samples(spec, label, &samples, source_timescale)?,
        ));
    }

    let mut anchors_by_offset = BTreeMap::<u64, u64>::new();
    for anchor in pts_anchors {
        match anchors_by_offset.insert(anchor.sample_offset, anchor.pts_90k) {
            Some(existing) if existing != anchor.pts_90k => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message: format!(
                        "{label} carried multiple conflicting PES timestamps for the same sample boundary"
                    ),
                });
            }
            _ => {}
        }
    }
    if samples.is_empty() || !anchors_by_offset.contains_key(&samples[0].data_offset) {
        return Ok((
            TRANSPORT_VIDEO_TIMESCALE,
            floored_transport_audio_samples(spec, label, &samples, source_timescale)?,
        ));
    }

    let intrinsic = floored_transport_audio_samples(spec, label, &samples, source_timescale)?;
    let mut anchored_sample_indexes = Vec::with_capacity(anchors_by_offset.len());
    for (anchor_offset, pts_90k) in anchors_by_offset {
        let sample_index = samples
            .partition_point(|sample| sample.data_offset < anchor_offset)
            .min(samples.len() - 1);
        anchored_sample_indexes.push((sample_index, pts_90k));
    }
    anchored_sample_indexes.dedup_by_key(|(index, _)| *index);
    if anchored_sample_indexes.len() >= 2 {
        let mut durations = intrinsic
            .iter()
            .map(|sample| sample.duration)
            .collect::<Vec<_>>();
        for anchor_window in anchored_sample_indexes.windows(2) {
            let (start_index, current_pts) = anchor_window[0];
            let (end_index, next_pts) = anchor_window[1];
            if end_index <= start_index {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message: format!(
                        "{label} carried non-monotonic PES anchor ordering across sample boundaries"
                    ),
                });
            }
            let anchored_span_duration =
                wrapped_transport_audio_anchor_delta(current_pts, next_pts)?;
            let prefix_duration = durations[start_index..end_index - 1]
                .iter()
                .fold(0_u64, |total, duration| total + u64::from(*duration));
            let residual_duration = u64::from(anchored_span_duration)
                .checked_sub(prefix_duration)
                .ok_or_else(|| MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message: format!(
                        "{label} carried PES anchors that resolve to a smaller duration than the intrinsic sample cadence"
                    ),
                })?;
            durations[end_index - 1] = u32::try_from(residual_duration).map_err(|_| {
                MuxError::LayoutOverflow("transport-stream timestamped audio anchored duration")
            })?;
        }
        let rescaled = samples
            .iter()
            .zip(durations)
            .map(|(sample, duration)| {
                rescaled_transport_audio_sample(spec, label, sample, duration, source_timescale)
            })
            .collect::<Result<Vec<_>, MuxError>>()?;
        return Ok((TRANSPORT_VIDEO_TIMESCALE, rescaled));
    }

    Ok((TRANSPORT_VIDEO_TIMESCALE, intrinsic))
}

fn transport_anchor_sample_indexes(
    spec: &str,
    label: &str,
    samples: &[StagedSample],
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<Option<Vec<(usize, u64)>>, MuxError> {
    if pts_anchors.is_empty() || samples.is_empty() {
        return Ok(None);
    }

    let mut anchors_by_offset = BTreeMap::<u64, u64>::new();
    for anchor in pts_anchors {
        match anchors_by_offset.insert(anchor.sample_offset, anchor.pts_90k) {
            Some(existing) if existing != anchor.pts_90k => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message: format!(
                        "{label} carried multiple conflicting PES timestamps for the same sample boundary"
                    ),
                });
            }
            _ => {}
        }
    }
    if !anchors_by_offset.contains_key(&samples[0].data_offset) {
        return Ok(None);
    }

    let mut anchored_sample_indexes = Vec::with_capacity(anchors_by_offset.len());
    for (anchor_offset, pts_90k) in anchors_by_offset {
        let sample_index = samples
            .partition_point(|sample| sample.data_offset < anchor_offset)
            .min(samples.len() - 1);
        anchored_sample_indexes.push((sample_index, pts_90k));
    }
    anchored_sample_indexes.dedup_by_key(|(index, _)| *index);
    Ok(Some(anchored_sample_indexes))
}

type TransportAnchoredAc3Samples = (u32, Vec<CandidateSample>, Option<Vec<u32>>);

fn build_transport_ac3_packet_anchored_samples(
    spec: &str,
    samples: Vec<StagedSample>,
    source_timescale: u32,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<TransportAnchoredAc3Samples, MuxError> {
    let Some(anchored_sample_indexes) = transport_anchor_sample_indexes(
        spec,
        "transport-stream AC-3 audio",
        &samples,
        pts_anchors,
    )?
    else {
        return Ok((
            TRANSPORT_VIDEO_TIMESCALE,
            samples
                .iter()
                .map(|sample| {
                    let duration = u64::from(sample.duration)
                        .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
                        .ok_or(MuxError::LayoutOverflow(
                            "transport-stream AC-3 duration rescale",
                        ))?
                        / u64::from(source_timescale);
                    Ok(CandidateSample {
                        source_index: usize::MAX,
                        data_offset: sample.data_offset,
                        data_size: sample.data_size,
                        duration: u32::try_from(duration).map_err(|_| {
                            MuxError::LayoutOverflow("transport-stream AC-3 duration")
                        })?,
                        composition_time_offset: 0,
                        is_sync_sample: sample.is_sync_sample,
                    })
                })
                .collect::<Result<Vec<_>, MuxError>>()?,
            None,
        ));
    };

    let intrinsic_duration = u32::try_from(
        u64::from(1_536_u32)
            .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream AC-3 duration rescale",
            ))?
            / u64::from(source_timescale),
    )
    .map_err(|_| MuxError::LayoutOverflow("transport-stream AC-3 duration"))?;
    let mut durations = vec![intrinsic_duration; samples.len()];
    let mut deferred_span_adjustment = 0_i64;
    for anchor_window in anchored_sample_indexes.windows(2) {
        let (start_index, current_pts) = anchor_window[0];
        let (end_index, next_pts) = anchor_window[1];
        if end_index <= start_index {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "transport-stream AC-3 carried non-monotonic PES anchor ordering across sample boundaries"
                        .to_string(),
            });
        }
        let anchored_span_duration = u32::try_from(
            (i128::from(next_pts) - i128::from(current_pts)).rem_euclid(1_i128 << 32),
        )
        .map_err(|_| MuxError::LayoutOverflow("transport-stream AC-3 anchored duration"))?;
        let sample_span = end_index - start_index;
        let expected_floor_span_duration = u32::try_from(
            u64::from(1_536_u32)
                .checked_mul(u64::try_from(sample_span).unwrap())
                .and_then(|value| value.checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE)))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream AC-3 anchored span duration",
                ))?
                / u64::from(source_timescale),
        )
        .map_err(|_| MuxError::LayoutOverflow("transport-stream AC-3 anchored duration"))?;
        let span_adjustment =
            i64::from(anchored_span_duration) - i64::from(expected_floor_span_duration);
        if anchored_span_duration.abs_diff(expected_floor_span_duration)
            <= TRANSPORT_AC3_ANCHOR_JITTER_TOLERANCE_90K
        {
            deferred_span_adjustment = deferred_span_adjustment
                .checked_add(span_adjustment)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream AC-3 anchored residual carry",
                ))?;
            continue;
        }
        let prefix_duration = u64::from(intrinsic_duration)
            .checked_mul(u64::try_from(sample_span - 1).unwrap())
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream AC-3 anchored span duration",
            ))?;
        let residual_duration = i64::try_from(
            u64::from(anchored_span_duration)
                .checked_sub(prefix_duration)
                .ok_or_else(|| MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream AC-3 carried PES anchors that resolve to a smaller duration than the intrinsic sample cadence"
                            .to_string(),
                })?,
        )
        .map_err(|_| MuxError::LayoutOverflow("transport-stream AC-3 duration"))?
        .checked_add(deferred_span_adjustment)
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream AC-3 anchored residual carry",
        ))?;
        if residual_duration <= 0 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "transport-stream AC-3 carried PES anchors that resolve to a smaller duration than the intrinsic sample cadence"
                        .to_string(),
            });
        }
        durations[end_index - 1] = u32::try_from(residual_duration)
            .map_err(|_| MuxError::LayoutOverflow("transport-stream AC-3 duration"))?;
        deferred_span_adjustment = 0;
    }

    let flat_chunk_sample_counts = build_transport_ac3_flat_chunk_sample_counts(spec, &durations)?;

    Ok((
        TRANSPORT_VIDEO_TIMESCALE,
        samples
            .iter()
            .zip(durations)
            .map(|(sample, duration)| CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration,
                composition_time_offset: 0,
                is_sync_sample: sample.is_sync_sample,
            })
            .collect(),
        Some(flat_chunk_sample_counts),
    ))
}

fn build_transport_ac3_flat_chunk_sample_counts(
    spec: &str,
    sample_durations: &[u32],
) -> Result<Vec<u32>, MuxError> {
    let mut counts = Vec::new();
    let mut current_count = 0_u32;
    let mut current_duration = 0_u64;
    let mut sample_index = 0usize;

    while sample_index < sample_durations.len() {
        let duration = u64::from(sample_durations[sample_index]);
        if current_count != 0
            && current_duration
                .checked_add(duration)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream AC-3 chunk duration",
                ))?
                > TRANSPORT_FLAT_AUDIO_INTERLEAVE_TARGET_90K
        {
            if duration > TRANSPORT_FLAT_AUDIO_INTERLEAVE_TARGET_90K {
                current_count = current_count
                    .checked_add(1)
                    .ok_or(MuxError::LayoutOverflow(
                        "transport-stream AC-3 chunk sample count",
                    ))?;
                counts.push(current_count);
                current_count = 0;
                current_duration = 0;
                sample_index += 1;
                if sample_index < sample_durations.len() {
                    counts.push(1);
                    sample_index += 1;
                }
                continue;
            }

            counts.push(current_count);
            current_count = 0;
            current_duration = 0;
            continue;
        }

        current_count = current_count
            .checked_add(1)
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream AC-3 chunk sample count",
            ))?;
        current_duration =
            current_duration
                .checked_add(duration)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream AC-3 chunk duration",
                ))?;
        sample_index += 1;
    }

    if current_count != 0 {
        counts.push(current_count);
    }
    if counts.is_empty() {
        return Err(MuxError::InvalidChunkPlan {
            track_id: 0,
            message: format!("{spec} produced no flat transport-stream AC-3 chunk boundaries"),
        });
    }
    Ok(counts)
}

fn finalize_transport_tracks_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    builders: BTreeMap<u16, TransportTrackBuilder>,
) -> Result<TransportStreamScanResult, MuxError> {
    if builders.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport stream input did not contain any supported native direct-ingest streams"
                    .to_string(),
        });
    }
    let mut tracks = Vec::new();
    let mut flat_chunk_sample_counts_by_track_id = BTreeMap::new();
    let mut skipped_empty_text_tracks = false;
    for (track_index, builder) in builders.into_values().enumerate() {
        if matches!(
            builder.kind,
            TransportTrackKind::DvbSubtitle | TransportTrackKind::DvbTeletext
        ) && builder.sample_offsets.is_empty()
        {
            skipped_empty_text_tracks = true;
            continue;
        }
        let finalized = match builder.kind {
            TransportTrackKind::Mp3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mp3_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Aac => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_aac_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Latm => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_latm_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Mhas => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mhas_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Ac3 => {
                finalize_transport_ac3_track_sync(path, spec, file, track_index, builder)?
            }
            TransportTrackKind::Truehd => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_truehd_track_sync(path, spec, file, track_index, builder)?,
                )
            }
            TransportTrackKind::Eac3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_eac3_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Ac4 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_ac4_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Dts => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_dts_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Mpeg2v => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_mpeg2v_track_sync(path, spec, file, track_index, builder)?,
                )
            }
            TransportTrackKind::Av1 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_av1_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Avs3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_avs3_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Mp4v => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mp4v_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::H264 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_h264_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::H265 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_h265_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::Vvc => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_vvc_track_sync(path, spec, file, track_index, builder)?,
            ),
            TransportTrackKind::DvbSubtitle => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_dvb_subtitle_track_sync(path, spec, track_index, builder)?,
                )
            }
            TransportTrackKind::DvbTeletext => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_dvb_teletext_track_sync(path, spec, track_index, builder)?,
                )
            }
        };
        if let Some(chunk_sample_counts) = finalized.flat_chunk_sample_counts {
            flat_chunk_sample_counts_by_track_id.insert(
                finalized.composite_track.track.track_id,
                chunk_sample_counts,
            );
        }
        tracks.push(finalized.composite_track);
    }
    if tracks.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: if skipped_empty_text_tracks {
                "transport stream input did not contain any subtitle or teletext PES payload units"
                    .to_string()
            } else {
                "transport stream input did not contain any supported native direct-ingest streams"
                    .to_string()
            },
        });
    }
    Ok(TransportStreamScanResult {
        composite_tracks: tracks,
        flat_chunk_sample_counts_by_track_id,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_tracks_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    builders: BTreeMap<u16, TransportTrackBuilder>,
) -> Result<TransportStreamScanResult, MuxError> {
    if builders.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport stream input did not contain any supported native direct-ingest streams"
                    .to_string(),
        });
    }
    let mut tracks = Vec::new();
    let mut flat_chunk_sample_counts_by_track_id = BTreeMap::new();
    let mut skipped_empty_text_tracks = false;
    for (track_index, builder) in builders.into_values().enumerate() {
        if matches!(
            builder.kind,
            TransportTrackKind::DvbSubtitle | TransportTrackKind::DvbTeletext
        ) && builder.sample_offsets.is_empty()
        {
            skipped_empty_text_tracks = true;
            continue;
        }
        let finalized = match builder.kind {
            TransportTrackKind::Mp3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mp3_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Aac => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_aac_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Latm => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_latm_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Mhas => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mhas_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Ac3 => {
                finalize_transport_ac3_track_async(path, spec, file, track_index, builder).await?
            }
            TransportTrackKind::Truehd => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_truehd_track_async(path, spec, file, track_index, builder)
                        .await?,
                )
            }
            TransportTrackKind::Eac3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_eac3_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Ac4 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_ac4_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Dts => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_dts_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Mpeg2v => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_mpeg2v_track_async(path, spec, file, track_index, builder)
                        .await?,
                )
            }
            TransportTrackKind::Av1 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_av1_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Avs3 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_avs3_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Mp4v => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_mp4v_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::H264 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_h264_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::H265 => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_h265_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::Vvc => FinalizedTransportTrack::without_flat_chunk_sample_counts(
                finalize_transport_vvc_track_async(path, spec, file, track_index, builder).await?,
            ),
            TransportTrackKind::DvbSubtitle => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_dvb_subtitle_track_async(path, spec, track_index, builder)
                        .await?,
                )
            }
            TransportTrackKind::DvbTeletext => {
                FinalizedTransportTrack::without_flat_chunk_sample_counts(
                    finalize_transport_dvb_teletext_track_async(path, spec, track_index, builder)
                        .await?,
                )
            }
        };
        if let Some(chunk_sample_counts) = finalized.flat_chunk_sample_counts {
            flat_chunk_sample_counts_by_track_id.insert(
                finalized.composite_track.track.track_id,
                chunk_sample_counts,
            );
        }
        tracks.push(finalized.composite_track);
    }
    if tracks.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: if skipped_empty_text_tracks {
                "transport stream input did not contain any subtitle or teletext PES payload units"
                    .to_string()
            } else {
                "transport stream input did not contain any supported native direct-ingest streams"
                    .to_string()
            },
        });
    }
    Ok(TransportStreamScanResult {
        composite_tracks: tracks,
        flat_chunk_sample_counts_by_track_id,
    })
}

fn finalize_transport_mp3_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let mut offset = 0_u64;
    let mut expected = None::<(u32, u16, u32)>;
    let mut samples = Vec::new();
    while offset < builder.total_size {
        if builder.total_size - offset < 4 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated MPEG audio frame header inside transport-stream payload"
                    .to_string(),
            });
        }
        let mut header = [0_u8; 4];
        read_segmented_bytes_sync(
            file,
            &builder.segments,
            builder.total_size,
            offset,
            &mut header,
            spec,
            "truncated MPEG audio frame header inside transport-stream payload",
        )?;
        let parsed = parse_mp3_frame_header(&header, offset, spec)?;
        if offset
            .checked_add(u64::from(parsed.frame_length))
            .is_none_or(|end| end > builder.total_size)
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: format!(
                    "truncated MPEG audio frame at logical transport-stream offset {offset}"
                ),
            });
        }
        let descriptor = (
            parsed.sample_rate,
            parsed.channel_count,
            parsed.sample_duration,
        );
        if let Some(expected) = expected {
            if expected != descriptor {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream MPEG audio frames changed sample rate or channel layout mid-stream"
                            .to_string(),
                });
            }
        } else {
            expected = Some(descriptor);
        }
        samples.push(StagedSample {
            data_offset: offset,
            data_size: parsed.frame_length,
            duration: parsed.sample_duration,
            composition_time_offset: 0,
            is_sync_sample: true,
        });
        offset =
            offset
                .checked_add(u64::from(parsed.frame_length))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream MPEG audio offset",
                ))?;
    }
    let (sample_rate, channel_count, _) =
        expected.ok_or_else(|| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport stream input did not contain any MPEG audio frames".to_string(),
        })?;
    let sample_entry_box = build_mp3_sample_entry_box(
        sample_rate,
        channel_count,
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
    )?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream MPEG audio",
        samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mp3"),
            mux_policy: direct_ingest_mux_policy("mp3", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_aac_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_adts_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream AAC audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("aac"),
            mux_policy: direct_ingest_mux_policy("aac", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_latm_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_latm_segmented_sync(file, &builder.segments, builder.total_size, path, spec)?;
    let super::latm::ParsedLatmTrack {
        sample_rate,
        sample_entry_box,
        segmented_source,
        samples: staged_samples,
    } = parsed;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream LATM audio",
        staged_samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("latm"),
            mux_policy: direct_ingest_mux_policy("latm", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: segmented_source,
    })
}

fn finalize_transport_mhas_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_mhas_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let super::mhas::ParsedMhasTrack {
        sample_rate,
        sample_entry_box: direct_sample_entry_box,
        samples: staged_samples,
        ..
    } = parsed;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream MHAS audio",
        staged_samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    let sample_entry_box = if timescale == sample_rate {
        direct_sample_entry_box
    } else {
        let btrt = build_btrt_from_sample_sizes(
            samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            timescale,
        )?;
        build_mhas_sample_entry_box_with_btrt(sample_rate, btrt)?
    };
    let sync_sample_table_mode = if samples.iter().all(|sample| sample.is_sync_sample) {
        super::super::SyncSampleTableMode::ForceFirstOnly
    } else {
        super::super::SyncSampleTableMode::Auto
    };
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mhas"),
            mux_policy: direct_ingest_mux_policy("mhas", MuxTrackKind::Audio)
                .with_sync_sample_table_mode(sync_sample_table_mode),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_mp4v_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_mp4v_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let transport_samples = rescale_transport_mp4v_samples(
        parsed.samples,
        parsed.timescale,
        &builder.pts_anchors,
        spec,
    )?;
    let total_duration_override = transport_mp4v_total_duration_override(&transport_samples);
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mp4v"),
            mux_policy: direct_ingest_mux_policy("mp4v", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box: build_direct_mp4v_sample_entry_box_with_total_duration(
                parsed.width,
                parsed.height,
                &parsed.decoder_specific_info,
                TRANSPORT_VIDEO_TIMESCALE,
                transport_samples
                    .iter()
                    .map(|sample| (sample.data_size, sample.duration)),
                total_duration_override,
            )?,
            source_edit_media_time: None,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_mpeg2v_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_mpeg2v_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let (transport_samples, source_edit_media_time) = build_transport_mpeg2v_samples(
        spec,
        parsed.samples,
        parsed.timescale,
        &builder.pts_anchors,
    )?;
    let sample_entry_box = build_transport_mpeg2v_sample_entry_box(
        parsed.width,
        parsed.height,
        &parsed.decoder_specific_info,
        parsed.object_type_indication,
        TRANSPORT_VIDEO_TIMESCALE,
        transport_samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        parsed.pixel_aspect_ratio,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mpeg2v"),
            mux_policy: direct_ingest_mux_policy("mpeg2v", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box,
            source_edit_media_time,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_av1_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let av1_descriptor = builder
        .av1_descriptor
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream AV1 track builder is missing its carried descriptor payload"
                .to_string(),
        })?;
    let parsed = scan_transport_av1_segmented_sync(
        path,
        file,
        &builder.segments,
        builder.total_size,
        &builder.sample_offsets,
        av1_descriptor,
        spec,
    )?;
    let samples = build_transport_av1_samples(spec, parsed.samples, &builder.pts_anchors)?;
    let source_spec = match parsed.source {
        super::av1::ParsedAv1TrackSource::Segmented(segmented) => segmented,
        super::av1::ParsedAv1TrackSource::File => return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AV1 direct ingest did not produce a segmented transformed source"
                    .to_string(),
        }),
    };
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("av1"),
            mux_policy: direct_ingest_mux_policy("av1", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec,
    })
}

fn finalize_transport_avs3_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let avs3_config = builder
        .avs3_config
        .as_deref()
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AVS3 track builder is missing its carried decoder configuration"
                    .to_string(),
        })?;
    let parsed = scan_transport_avs3_segmented_sync(
        file,
        &builder.segments,
        builder.total_size,
        &builder.sample_offsets,
        avs3_config,
        spec,
    )?;
    let transport_samples = rescale_transport_avs3_samples(parsed.samples, parsed.timescale, spec)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("avs3"),
            mux_policy: with_force_empty_sync_sample_table(direct_ingest_mux_policy(
                "avs3",
                MuxTrackKind::Video,
            )),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_ac3_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<FinalizedTransportTrack, MuxError> {
    let parsed = scan_ac3_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let (timescale, samples, flat_chunk_sample_counts) =
        build_transport_ac3_packet_anchored_samples(
            spec,
            parsed.samples,
            parsed.sample_rate,
            &builder.pts_anchors,
        )?;
    let sample_entry_box = build_ac3_sample_entry_box_with_btrt(
        &parsed.decoder_config,
        timescale,
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
    )?;
    Ok(FinalizedTransportTrack {
        composite_track: CompositeTrackCandidate {
            track: TrackCandidate {
                track_id: u32::from(builder.pid),
                kind: MuxTrackKind::Audio,
                timescale,
                language: *b"und",
                handler_name: direct_ingest_handler_name("ac3"),
                mux_policy: direct_ingest_mux_policy("ac3", MuxTrackKind::Audio),
                width: 0,
                height: 0,
                sample_entry_box,
                source_edit_media_time: None,
                samples,
            },
            source_spec: SegmentedMuxSourceSpec {
                path: path.to_path_buf(),
                segments: builder.segments,
                total_size: builder.total_size,
            },
        },
        flat_chunk_sample_counts,
    })
}

fn finalize_transport_truehd_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_truehd_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream TrueHD audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("truehd"),
            mux_policy: direct_ingest_mux_policy("truehd", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: build_truehd_sample_entry_box_with_btrt(
                parsed.descriptor,
                build_btrt_from_sample_sizes(
                    samples
                        .iter()
                        .map(|sample| (sample.data_size, sample.duration)),
                    timescale,
                )?,
            )?,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_eac3_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_eac3_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream E-AC-3 audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    let rebuilt_sample_entry_box = build_eac3_sample_entry_box_with_btrt(
        &parsed.decoder_config,
        build_btrt_from_sample_sizes(
            samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            timescale,
        )?,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("eac3"),
            mux_policy: direct_ingest_mux_policy("eac3", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: rebuilt_sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_ac4_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_ac4_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale: parsed.media_time_scale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("ac4"),
            mux_policy: direct_ingest_mux_policy("ac4", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples: parsed
                .samples
                .into_iter()
                .map(|sample| CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration: sample.duration,
                    composition_time_offset: sample.composition_time_offset,
                    is_sync_sample: sample.is_sync_sample,
                })
                .collect(),
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_dts_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed = scan_dts_segmented_sync(file, &builder.segments, builder.total_size, spec)?;
    let sample_entry_box = retune_carried_dts_sample_entry_box(&parsed.sample_entry_box)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale: parsed.media_timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("dts"),
            mux_policy: direct_ingest_mux_policy("dts", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples: parsed
                .samples
                .into_iter()
                .map(|sample| CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration: sample.duration,
                    composition_time_offset: sample.composition_time_offset,
                    is_sync_sample: sample.is_sync_sample,
                })
                .collect(),
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_h264_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_h264_segmented_sync(path, file, &builder.segments, builder.total_size, spec)?;
    let mut samples =
        rescale_transport_h26x_samples(parsed.samples, parsed.timescale, spec, "H.264")?;
    let mut source_edit_media_time = rescale_transport_h26x_edit_media_time(
        parsed.source_edit_media_time,
        parsed.timescale,
        spec,
        "H.264",
    )?;
    if source_edit_media_time.unwrap_or(0) != 0
        || samples
            .iter()
            .any(|sample| sample.composition_time_offset != 0)
    {
        align_transport_h264_presentation_time(
            &mut samples,
            &mut source_edit_media_time,
            &builder.pts_anchors,
        )?;
        normalize_transport_h264_wraparound_samples(&mut samples, &mut source_edit_media_time)?;
    } else {
        source_edit_media_time = None;
    }
    let sample_entry_box = retune_carried_h264_sample_entry_box(
        &parsed.sample_entry_box,
        TRANSPORT_VIDEO_TIMESCALE,
        Some(authored_h264_media_duration(samples.iter().map(
            |sample| (sample.duration, sample.composition_time_offset),
        ))?),
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        true,
        transport_h264_sample_entry_has_colr(&parsed.sample_entry_box)?,
    )?;
    let sample_entry_box = ensure_transport_h264_colorized_sample_entry(&sample_entry_box)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("h264"),
            mux_policy: direct_ingest_mux_policy("h264", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box,
            source_edit_media_time,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

fn finalize_transport_h265_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_h265_segmented_sync(path, file, &builder.segments, builder.total_size, spec)?;
    let samples = rescale_transport_h26x_samples(parsed.samples, parsed.timescale, spec, "H.265")?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("h265"),
            mux_policy: direct_ingest_mux_policy("h265", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: rescale_transport_h26x_edit_media_time(
                parsed.source_edit_media_time,
                parsed.timescale,
                spec,
                "H.265",
            )?,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

fn finalize_transport_vvc_track_sync(
    path: &Path,
    spec: &str,
    file: &mut File,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_vvc_segmented_sync(path, file, &builder.segments, builder.total_size, spec)?;
    let samples = build_transport_vvc_samples(spec, parsed.samples, &builder.pts_anchors)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("vvc"),
            mux_policy: direct_ingest_mux_policy("vvc", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_mp3_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let mut offset = 0_u64;
    let mut expected = None::<(u32, u16, u32)>;
    let mut samples = Vec::new();
    while offset < builder.total_size {
        if builder.total_size - offset < 4 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "truncated MPEG audio frame header inside transport-stream payload"
                    .to_string(),
            });
        }
        let mut header = [0_u8; 4];
        read_segmented_bytes_async(
            file,
            &builder.segments,
            builder.total_size,
            offset,
            &mut header,
            spec,
            "truncated MPEG audio frame header inside transport-stream payload",
        )
        .await?;
        let parsed = parse_mp3_frame_header(&header, offset, spec)?;
        if offset
            .checked_add(u64::from(parsed.frame_length))
            .is_none_or(|end| end > builder.total_size)
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: format!(
                    "truncated MPEG audio frame at logical transport-stream offset {offset}"
                ),
            });
        }
        let descriptor = (
            parsed.sample_rate,
            parsed.channel_count,
            parsed.sample_duration,
        );
        if let Some(expected) = expected {
            if expected != descriptor {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream MPEG audio frames changed sample rate or channel layout mid-stream"
                            .to_string(),
                });
            }
        } else {
            expected = Some(descriptor);
        }
        samples.push(StagedSample {
            data_offset: offset,
            data_size: parsed.frame_length,
            duration: parsed.sample_duration,
            composition_time_offset: 0,
            is_sync_sample: true,
        });
        offset =
            offset
                .checked_add(u64::from(parsed.frame_length))
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream MPEG audio offset",
                ))?;
    }
    let (sample_rate, channel_count, _) =
        expected.ok_or_else(|| MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport stream input did not contain any MPEG audio frames".to_string(),
        })?;
    let sample_entry_box = build_mp3_sample_entry_box(
        sample_rate,
        channel_count,
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
    )?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream MPEG audio",
        samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mp3"),
            mux_policy: direct_ingest_mux_policy("mp3", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_aac_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_adts_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream AAC audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("aac"),
            mux_policy: direct_ingest_mux_policy("aac", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_latm_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_latm_segmented_async(file, &builder.segments, builder.total_size, path, spec).await?;
    let super::latm::ParsedLatmTrack {
        sample_rate,
        sample_entry_box,
        segmented_source,
        samples: staged_samples,
    } = parsed;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream LATM audio",
        staged_samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("latm"),
            mux_policy: direct_ingest_mux_policy("latm", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: segmented_source,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_mhas_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_mhas_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let super::mhas::ParsedMhasTrack {
        sample_rate,
        sample_entry_box: direct_sample_entry_box,
        samples: staged_samples,
        ..
    } = parsed;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream MHAS audio",
        staged_samples,
        sample_rate,
        &builder.pts_anchors,
    )?;
    let sample_entry_box = if timescale == sample_rate {
        direct_sample_entry_box
    } else {
        let btrt = build_btrt_from_sample_sizes(
            samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            timescale,
        )?;
        build_mhas_sample_entry_box_with_btrt(sample_rate, btrt)?
    };
    let sync_sample_table_mode = if samples.iter().all(|sample| sample.is_sync_sample) {
        super::super::SyncSampleTableMode::ForceFirstOnly
    } else {
        super::super::SyncSampleTableMode::Auto
    };
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mhas"),
            mux_policy: direct_ingest_mux_policy("mhas", MuxTrackKind::Audio)
                .with_sync_sample_table_mode(sync_sample_table_mode),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_mp4v_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_mp4v_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let transport_samples = rescale_transport_mp4v_samples(
        parsed.samples,
        parsed.timescale,
        &builder.pts_anchors,
        spec,
    )?;
    let total_duration_override = transport_mp4v_total_duration_override(&transport_samples);
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mp4v"),
            mux_policy: direct_ingest_mux_policy("mp4v", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box: build_direct_mp4v_sample_entry_box_with_total_duration(
                parsed.width,
                parsed.height,
                &parsed.decoder_specific_info,
                TRANSPORT_VIDEO_TIMESCALE,
                transport_samples
                    .iter()
                    .map(|sample| (sample.data_size, sample.duration)),
                total_duration_override,
            )?,
            source_edit_media_time: None,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_mpeg2v_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_mpeg2v_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let (transport_samples, source_edit_media_time) = build_transport_mpeg2v_samples(
        spec,
        parsed.samples,
        parsed.timescale,
        &builder.pts_anchors,
    )?;
    let sample_entry_box = build_transport_mpeg2v_sample_entry_box(
        parsed.width,
        parsed.height,
        &parsed.decoder_specific_info,
        parsed.object_type_indication,
        TRANSPORT_VIDEO_TIMESCALE,
        transport_samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        parsed.pixel_aspect_ratio,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("mpeg2v"),
            mux_policy: direct_ingest_mux_policy("mpeg2v", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box,
            source_edit_media_time,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_av1_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let av1_descriptor = builder
        .av1_descriptor
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream AV1 track builder is missing its carried descriptor payload"
                .to_string(),
        })?;
    let parsed = scan_transport_av1_segmented_async(
        path,
        file,
        &builder.segments,
        builder.total_size,
        &builder.sample_offsets,
        av1_descriptor,
        spec,
    )
    .await?;
    let samples = build_transport_av1_samples(spec, parsed.samples, &builder.pts_anchors)?;
    let source_spec = match parsed.source {
        super::av1::ParsedAv1TrackSource::Segmented(segmented) => segmented,
        super::av1::ParsedAv1TrackSource::File => return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AV1 direct ingest did not produce a segmented transformed source"
                    .to_string(),
        }),
    };
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("av1"),
            mux_policy: direct_ingest_mux_policy("av1", MuxTrackKind::Video),
            width: parsed.width,
            height: parsed.height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_avs3_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let avs3_config = builder
        .avs3_config
        .as_deref()
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AVS3 track builder is missing its carried decoder configuration"
                    .to_string(),
        })?;
    let parsed = scan_transport_avs3_segmented_async(
        file,
        &builder.segments,
        builder.total_size,
        &builder.sample_offsets,
        avs3_config,
        spec,
    )
    .await?;
    let transport_samples = rescale_transport_avs3_samples(parsed.samples, parsed.timescale, spec)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("avs3"),
            mux_policy: with_force_empty_sync_sample_table(direct_ingest_mux_policy(
                "avs3",
                MuxTrackKind::Video,
            )),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples: transport_samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn rescale_transport_mpeg2v_samples(
    samples: Vec<StagedSample>,
    source_timescale: u32,
    spec: &str,
) -> Result<Vec<CandidateSample>, MuxError> {
    samples
        .into_iter()
        .map(|sample| {
            Ok(CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration: rescale_transport_mpeg2v_time(
                    i64::from(sample.duration),
                    source_timescale,
                    spec,
                )? as u32,
                composition_time_offset: rescale_transport_mpeg2v_time(
                    i64::from(sample.composition_time_offset),
                    source_timescale,
                    spec,
                )? as i32,
                is_sync_sample: sample.is_sync_sample,
            })
        })
        .collect()
}

fn build_transport_mpeg2v_samples(
    spec: &str,
    samples: Vec<StagedSample>,
    source_timescale: u32,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<(Vec<CandidateSample>, Option<u64>), MuxError> {
    if pts_anchors.is_empty() {
        return Ok((
            rescale_transport_mpeg2v_samples(samples, source_timescale, spec)?,
            None,
        ));
    }

    if pts_anchors.len() == 1 && samples.len() > 1 {
        let duration = TRANSPORT_VIDEO_TIMESCALE / 30;
        let transport_samples = samples
            .into_iter()
            .map(|sample| CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration,
                composition_time_offset: 0,
                is_sync_sample: sample.is_sync_sample,
            })
            .collect();
        return Ok((transport_samples, None));
    }

    let mut transport_samples = Vec::with_capacity(samples.len());
    let mut constant_composition_offset = None::<i32>;
    for sample in samples {
        let duration = u32::try_from(rescale_transport_mpeg2v_time(
            i64::from(sample.duration),
            source_timescale,
            spec,
        )?)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream MPEG-2 video duration"))?;
        let composition_time_offset = match constant_composition_offset {
            Some(value) => value,
            None => {
                let value = i32::try_from(duration).map_err(|_| {
                    MuxError::LayoutOverflow("transport-stream MPEG-2 video composition offset")
                })?;
                constant_composition_offset = Some(value);
                value
            }
        };
        transport_samples.push(CandidateSample {
            source_index: usize::MAX,
            data_offset: sample.data_offset,
            data_size: sample.data_size,
            duration,
            composition_time_offset,
            is_sync_sample: sample.is_sync_sample,
        });
    }
    let source_edit_media_time =
        constant_composition_offset.map(|offset| u64::try_from(offset).unwrap_or(u64::MAX));
    Ok((transport_samples, source_edit_media_time))
}

fn rescale_transport_h26x_samples(
    samples: Vec<StagedSample>,
    source_timescale: u32,
    spec: &str,
    codec_name: &str,
) -> Result<Vec<CandidateSample>, MuxError> {
	let mut decode_time: i64 = 0;
	let mut output: Vec<CandidateSample> = Vec::with_capacity(samples.len());
	for sample in samples {
		let end_time: i64 = decode_time.checked_add(i64::from(sample.duration))
			.ok_or(MuxError::LayoutOverflow("transport-stream H26x decode time"))?;
		let presentation_time: i64 = decode_time.checked_add(i64::from(sample.composition_time_offset))
			.ok_or(MuxError::LayoutOverflow("transport-stream H26x presentation time"))?;
		// Rescale absolute boundaries, not individual durations. For 24000/1001
		// fps, this distributes 3753/3754 ticks without accumulating drift.
		let start: i64 = rescale_transport_h26x_time(decode_time, source_timescale, spec, codec_name)?;
		let end: i64 = rescale_transport_h26x_time(end_time, source_timescale, spec, codec_name)?;
		let presentation: i64 = rescale_transport_h26x_time(presentation_time, source_timescale, spec, codec_name)?;
		let duration: u32 = u32::try_from(end - start)
			.map_err(|_| MuxError::LayoutOverflow("transport-stream H26x duration rescale"))?;
		if duration == 0 {
			return Err(MuxError::UnsupportedTrackImport {
				spec: spec.to_string(),
				message: format!("transport-stream {codec_name} sample duration is below one 90_000 clock tick"),
			});
		}
		output.push(CandidateSample {
			source_index: usize::MAX,
			data_offset: sample.data_offset,
			data_size: sample.data_size,
			duration,
			composition_time_offset: i32::try_from(presentation - start)
				.map_err(|_| MuxError::LayoutOverflow("transport-stream H26x composition offset rescale"))?,
			is_sync_sample: sample.is_sync_sample,
		});
		decode_time = end_time;
	}
	Ok(output)
}

fn rescale_transport_h26x_edit_media_time(
    source_edit_media_time: Option<u64>,
    source_timescale: u32,
    spec: &str,
    codec_name: &str,
) -> Result<Option<u64>, MuxError> {
    source_edit_media_time
        .map(|value| {
            u64::try_from(rescale_transport_h26x_time(
                i64::try_from(value)
                    .map_err(|_| MuxError::LayoutOverflow("transport-stream edit-media time"))?,
                source_timescale,
                spec,
                codec_name,
            )?)
            .map_err(|_| MuxError::LayoutOverflow("transport-stream edit-media time"))
        })
        .transpose()
}

fn align_transport_h264_presentation_time(
    samples: &mut [CandidateSample],
    source_edit_media_time: &mut Option<u64>,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<(), MuxError> {
    let Some(first_sample) = samples.first() else {
        return Ok(());
    };
    let Some(first_anchor_pts) = pts_anchors
        .iter()
        .find(|anchor| anchor.sample_offset == first_sample.data_offset)
        .map(|anchor| anchor.pts_90k)
    else {
        return Ok(());
    };
    let current_edit_media_time = i64::try_from(source_edit_media_time.unwrap_or(0))
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 edit-media time"))?;
    let target_edit_media_time = i64::try_from(first_anchor_pts)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 edit-media time"))?;
    let delta = wrapped_transport_h264_edit_delta(target_edit_media_time, current_edit_media_time)?;
    let delta = i32::try_from(delta)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 composition realignment"))?;
    for sample in samples.iter_mut() {
        sample.composition_time_offset =
            sample
                .composition_time_offset
                .checked_add(delta)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream H.264 composition realignment",
                ))?;
    }
    let negative_shift = samples
        .iter()
        .map(|sample| sample.composition_time_offset)
        .min()
        .unwrap_or(0)
        .min(0)
        .unsigned_abs();
    if negative_shift != 0 {
        let shift = i32::try_from(negative_shift)
            .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 composition shift"))?;
        for sample in samples.iter_mut() {
            sample.composition_time_offset =
                sample.composition_time_offset.checked_add(shift).ok_or(
                    MuxError::LayoutOverflow("transport-stream H.264 composition shift"),
                )?;
        }
    }
    *source_edit_media_time = Some(
        first_anchor_pts
            .checked_add(u64::from(negative_shift))
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream H.264 edit-media time",
            ))?,
    );
    Ok(())
}

fn normalize_transport_h264_wraparound_samples(
    samples: &mut Vec<CandidateSample>,
    source_edit_media_time: &mut Option<u64>,
) -> Result<(), MuxError> {
    let Some(current_edit_media_time) = *source_edit_media_time else {
        return Ok(());
    };
    let Some(first_duration) = samples.first().map(|sample| sample.duration) else {
        return Ok(());
    };
    if first_duration == 0 {
        return Ok(());
    }
    let frame_duration = u64::from(first_duration);
    if current_edit_media_time < TRANSPORT_MAX_PCR_90K.saturating_sub(frame_duration) {
        return Ok(());
    }

    let shift = i32::try_from(first_duration)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 wrap composition shift"))?;
    for sample in samples.iter_mut() {
        sample.composition_time_offset =
            sample
                .composition_time_offset
                .checked_add(shift)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream H.264 wrap composition shift",
                ))?;
    }

    let normalized_edit_media_time = current_edit_media_time % TRANSPORT_MAX_PCR_90K;
    *source_edit_media_time = Some(
        normalized_edit_media_time
            .checked_add(frame_duration)
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream H.264 wrap edit-media time",
            ))?,
    );

    let mut collapse_index = None;
    for index in 1..samples.len().saturating_sub(1) {
        let previous = &samples[index - 1];
        let current = &samples[index];
        let next = &samples[index + 1];
        if !current.is_sync_sample
            && next.is_sync_sample
            && previous.duration == first_duration
            && current.duration == first_duration
            && current.composition_time_offset == next.composition_time_offset
        {
            collapse_index = Some(index);
        }
    }
    if let Some(index) = collapse_index {
        let extra_duration = samples[index].duration;
        samples[index - 1].duration = samples[index - 1]
            .duration
            .checked_add(extra_duration)
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream H.264 wrap collapsed sample duration",
            ))?;
        samples.remove(index);
    }
    Ok(())
}

fn ensure_transport_h264_colorized_sample_entry(
    sample_entry_box: &[u8],
) -> Result<Vec<u8>, MuxError> {
    let child_boxes = super::super::mp4::visual_sample_entry_immediate_children(sample_entry_box)?;
    let sample_entry_type = FourCc::from_bytes(
        sample_entry_box
            .get(4..8)
            .ok_or(MuxError::LayoutOverflow(
                "transport-stream H.264 sample-entry type",
            ))?
            .try_into()
            .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 sample-entry type"))?,
    );
    let mut avcc_box = None::<Vec<u8>>;
    let mut btrt_box = None::<Vec<u8>>;
    let mut colr_box = None::<Vec<u8>>;
    let mut preserved_other_boxes = Vec::new();
    for child_box in child_boxes {
        let child_type = FourCc::from_bytes(
            child_box
                .get(4..8)
                .ok_or(MuxError::LayoutOverflow(
                    "transport-stream H.264 sample-entry child type",
                ))?
                .try_into()
                .map_err(|_| {
                    MuxError::LayoutOverflow("transport-stream H.264 sample-entry child type")
                })?,
        );
        match child_type {
            value if value == FourCc::from_bytes(*b"avcC") => avcc_box = Some(child_box),
            value if value == FourCc::from_bytes(*b"btrt") => btrt_box = Some(child_box),
            value if value == FourCc::from_bytes(*b"colr") => colr_box = Some(child_box),
            _ => preserved_other_boxes.push(child_box),
        }
    }
    let avcc_box = avcc_box.ok_or_else(|| MuxError::UnsupportedTrackImport {
        spec: "ts".to_string(),
        message:
            "transport-stream H.264 sample entry did not contain an `avcC` decoder configuration box"
                .to_string(),
    })?;
    let avcc = super::super::mp4::decode_typed_box::<AVCDecoderConfiguration>(&avcc_box)?;
    let (rebuilt_sample_entry_box, _, _) =
        build_h264_sample_entry_from_avc_config_with_box_type_and_options(
            &avcc,
            sample_entry_type,
            "track",
            false,
        )?;
    let mut rebuilt_children =
        super::super::mp4::visual_sample_entry_immediate_children(&rebuilt_sample_entry_box)?;
    if let Some(colr_box) = colr_box {
        rebuilt_children.push(colr_box);
    }
    rebuilt_children.extend(preserved_other_boxes);
    if let Some(btrt_box) = btrt_box {
        rebuilt_children.push(btrt_box);
    }
    super::super::mp4::replace_visual_sample_entry_immediate_children(
        &rebuilt_sample_entry_box,
        &rebuilt_children,
    )
}

fn transport_h264_sample_entry_has_colr(sample_entry_box: &[u8]) -> Result<bool, MuxError> {
    Ok(
        super::super::mp4::visual_sample_entry_immediate_children(sample_entry_box)?
            .iter()
            .any(|child_box| child_box.get(4..8) == Some(&b"colr"[..])),
    )
}

fn wrapped_transport_h264_edit_delta(
    target_edit_media_time: i64,
    current_edit_media_time: i64,
) -> Result<i64, MuxError> {
    let direct = i128::from(target_edit_media_time) - i128::from(current_edit_media_time);
    let wrap = i128::from(TRANSPORT_MAX_PCR_90K);
    let best = [direct, direct - wrap, direct + wrap]
        .into_iter()
        .min_by_key(|candidate| candidate.abs())
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream H.264 edit-media time realignment",
        ))?;
    i64::try_from(best)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H.264 edit-media time realignment"))
}

fn rescale_transport_avs3_samples(
    samples: Vec<StagedSample>,
    source_timescale: u32,
    spec: &str,
) -> Result<Vec<CandidateSample>, MuxError> {
    samples
        .into_iter()
        .map(|sample| {
            Ok(CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration: rescale_transport_avs3_time(
                    i64::from(sample.duration),
                    source_timescale,
                    spec,
                )? as u32,
                composition_time_offset: rescale_transport_avs3_time(
                    i64::from(sample.composition_time_offset),
                    source_timescale,
                    spec,
                )? as i32,
                is_sync_sample: sample.is_sync_sample,
            })
        })
        .collect()
}

fn rescale_transport_mpeg2v_time(
    value: i64,
    source_timescale: u32,
    spec: &str,
) -> Result<i64, MuxError> {
    if source_timescale == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream MPEG-2 video used an invalid zero timescale".to_string(),
        });
    }

    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream MPEG-2 video time rescale",
        ))?;
    if scaled % u64::from(source_timescale) != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream MPEG-2 video cadence does not rescale cleanly onto the 90_000 media clock".to_string(),
        });
    }
    let normalized = scaled / u64::from(source_timescale);
    let normalized = i64::try_from(normalized)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream MPEG-2 video time rescale"))?;
    Ok(normalized * sign)
}

fn rescale_transport_h26x_time(
    value: i64,
    source_timescale: u32,
    spec: &str,
    codec_name: &str,
) -> Result<i64, MuxError> {
    if source_timescale == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!("transport-stream {codec_name} used an invalid zero timescale"),
        });
    }

    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream H26x time rescale",
        ))?;
	let rounded: u64 = scaled.checked_add(u64::from(source_timescale) / 2)
		.ok_or(MuxError::LayoutOverflow("transport-stream H26x time rescale"))?;
	let normalized: u64 = rounded / u64::from(source_timescale);
	let normalized: i64 = i64::try_from(normalized)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream H26x time rescale"))?;
    Ok(normalized * sign)
}

fn rescale_transport_avs3_time(
    value: i64,
    source_timescale: u32,
    spec: &str,
) -> Result<i64, MuxError> {
    if source_timescale == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream AVS3 video used an invalid zero timescale".to_string(),
        });
    }

    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(TRANSPORT_VIDEO_TIMESCALE))
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream AVS3 video time rescale",
        ))?;
    if scaled % u64::from(source_timescale) != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream AVS3 video cadence does not rescale cleanly onto the 90_000 media clock".to_string(),
        });
    }
    let normalized = scaled / u64::from(source_timescale);
    let normalized = i64::try_from(normalized)
        .map_err(|_| MuxError::LayoutOverflow("transport-stream AVS3 video time rescale"))?;
    Ok(normalized * sign)
}

fn build_transport_av1_samples(
    spec: &str,
    samples: Vec<StagedSample>,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<Vec<CandidateSample>, MuxError> {
    if pts_anchors.is_empty() {
        return Ok(samples
            .into_iter()
            .map(|sample| CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration: 0,
                composition_time_offset: 0,
                is_sync_sample: sample.is_sync_sample,
            })
            .collect());
    }
    if pts_anchors.len() != samples.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream AV1 PES timestamp anchors did not line up with access-unit boundaries"
                    .to_string(),
        });
    }

    samples
        .into_iter()
        .enumerate()
        .map(|(index, sample)| {
            let duration = if let Some(next_anchor) = pts_anchors.get(index + 1) {
                if next_anchor.pts_90k <= pts_anchors[index].pts_90k {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: spec.to_string(),
                        message:
                            "transport-stream AV1 carried non-monotonic PES timestamps across sample boundaries"
                                .to_string(),
                    });
                }
                u32::try_from(next_anchor.pts_90k - pts_anchors[index].pts_90k)
                    .map_err(|_| MuxError::LayoutOverflow("transport-stream AV1 duration"))?
            } else {
                0
            };
            Ok(CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration,
                composition_time_offset: 0,
                is_sync_sample: sample.is_sync_sample,
            })
        })
        .collect()
}

fn build_transport_vvc_samples(
    spec: &str,
    samples: Vec<StagedSample>,
    pts_anchors: &[TransportTimestampAnchor],
) -> Result<Vec<CandidateSample>, MuxError> {
    fn zero_duration_transport_vvc_samples(samples: &[StagedSample]) -> Vec<CandidateSample> {
        samples
            .iter()
            .map(|sample| CandidateSample {
                source_index: usize::MAX,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration: 0,
                composition_time_offset: 0,
                is_sync_sample: sample.is_sync_sample,
            })
            .collect()
    }

    if pts_anchors.is_empty() {
        return Ok(zero_duration_transport_vvc_samples(&samples));
    }

    let mut anchors_by_offset = BTreeMap::<u64, u64>::new();
    for anchor in pts_anchors {
        match anchors_by_offset.insert(anchor.sample_offset, anchor.pts_90k) {
            Some(existing) if existing != anchor.pts_90k => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream VVC video carried multiple conflicting PES timestamps for the same sample boundary"
                            .to_string(),
                });
            }
            _ => {}
        }
    }

    if samples.is_empty() || !anchors_by_offset.contains_key(&samples[0].data_offset) {
        return Ok(zero_duration_transport_vvc_samples(&samples));
    }

    if samples
        .iter()
        .all(|sample| anchors_by_offset.contains_key(&sample.data_offset))
    {
        return samples
            .iter()
            .enumerate()
            .map(|(index, sample)| {
                let duration = if let Some(next_sample) = samples.get(index + 1) {
                    let current_pts = anchors_by_offset[&sample.data_offset];
                    let next_pts = anchors_by_offset[&next_sample.data_offset];
                    if next_pts <= current_pts {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec: spec.to_string(),
                            message:
                                "transport-stream VVC video carried non-monotonic PES timestamps across sample boundaries"
                                    .to_string(),
                        });
                    }
                    u32::try_from(next_pts - current_pts)
                        .map_err(|_| MuxError::LayoutOverflow("transport-stream VVC duration"))?
                } else {
                    0
                };
                Ok(CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration,
                    composition_time_offset: 0,
                    is_sync_sample: sample.is_sync_sample,
                })
            })
            .collect();
    }

    if pts_anchors.len() == samples.len() {
        return samples
            .iter()
            .enumerate()
            .map(|(index, sample)| {
                let duration = if let Some(next_anchor) = pts_anchors.get(index + 1) {
                    if next_anchor.pts_90k <= pts_anchors[index].pts_90k {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec: spec.to_string(),
                            message:
                                "transport-stream VVC video carried non-monotonic PES timestamps across sample boundaries"
                                    .to_string(),
                        });
                    }
                    u32::try_from(next_anchor.pts_90k - pts_anchors[index].pts_90k)
                        .map_err(|_| MuxError::LayoutOverflow("transport-stream VVC duration"))?
                } else {
                    0
                };
                Ok(CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration,
                    composition_time_offset: 0,
                    is_sync_sample: sample.is_sync_sample,
                })
            })
            .collect();
    }

    Ok(zero_duration_transport_vvc_samples(&samples))
}

fn rescale_transport_mp4v_samples(
    samples: Vec<StagedSample>,
    _source_timescale: u32,
    pts_anchors: &[TransportTimestampAnchor],
    spec: &str,
) -> Result<Vec<CandidateSample>, MuxError> {
    let fallback_base_duration = samples
        .iter()
        .find_map(|sample| (sample.duration != 0).then_some(sample.duration))
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "transport-stream MPEG-4 Part 2 video did not expose a non-zero frame cadence"
                .to_string(),
        })?;

    fn fallback_transport_mp4v_sample(
        spec: &str,
        fallback_base_duration: u32,
        sample: &StagedSample,
    ) -> Result<CandidateSample, MuxError> {
        Ok(CandidateSample {
            source_index: usize::MAX,
            data_offset: sample.data_offset,
            data_size: sample.data_size,
            duration: u32::try_from(transport_mp4v_fallback_time(
                i64::from(sample.duration),
                fallback_base_duration,
                spec,
            )?)
            .map_err(|_| MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 duration"))?,
            composition_time_offset: i32::try_from(transport_mp4v_fallback_time(
                i64::from(sample.composition_time_offset),
                fallback_base_duration,
                spec,
            )?)
            .map_err(|_| {
                MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 composition offset")
            })?,
            is_sync_sample: sample.is_sync_sample,
        })
    }

    fn fallback_transport_mp4v_samples(
        spec: &str,
        fallback_base_duration: u32,
        samples: &[StagedSample],
    ) -> Result<Vec<CandidateSample>, MuxError> {
        samples
            .iter()
            .map(|sample| fallback_transport_mp4v_sample(spec, fallback_base_duration, sample))
            .collect()
    }

    if pts_anchors.is_empty() {
        return fallback_transport_mp4v_samples(spec, fallback_base_duration, &samples);
    }

    let mut anchors_by_offset = BTreeMap::<u64, u64>::new();
    for anchor in pts_anchors {
        match anchors_by_offset.insert(anchor.sample_offset, anchor.pts_90k) {
            Some(existing) if existing != anchor.pts_90k => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: spec.to_string(),
                    message:
                        "transport-stream MPEG-4 Part 2 video carried multiple conflicting PES timestamps for the same sample boundary".to_string(),
                });
            }
            _ => {}
        }
    }

    if samples.is_empty() || !anchors_by_offset.contains_key(&samples[0].data_offset) {
        return fallback_transport_mp4v_samples(spec, fallback_base_duration, &samples);
    }

    if samples
        .iter()
        .all(|sample| anchors_by_offset.contains_key(&sample.data_offset))
    {
        return samples
            .iter()
            .enumerate()
            .map(|(index, sample)| {
                let current_pts = anchors_by_offset[&sample.data_offset];
                let duration = if let Some(next_sample) = samples.get(index + 1) {
                    let next_pts = anchors_by_offset[&next_sample.data_offset];
                    if next_pts <= current_pts {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec: spec.to_string(),
                            message:
                                "transport-stream MPEG-4 Part 2 video carried non-monotonic PES timestamps across sample boundaries".to_string(),
                        });
                    }
                    u32::try_from(next_pts - current_pts).map_err(|_| {
                        MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 duration")
                    })?
                } else {
                    u32::try_from(transport_mp4v_fallback_time(
                        i64::from(sample.duration),
                        fallback_base_duration,
                        spec,
                    )?)
                    .map_err(|_| {
                        MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 duration")
                    })?
                };
                Ok(CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration,
                    composition_time_offset: i32::try_from(transport_mp4v_fallback_time(
                        i64::from(sample.composition_time_offset),
                        fallback_base_duration,
                        spec,
                    )?)
                    .map_err(|_| {
                        MuxError::LayoutOverflow(
                            "transport-stream MPEG-4 Part 2 composition offset",
                        )
                    })?,
                    is_sync_sample: sample.is_sync_sample,
                })
            })
            .collect();
    }

    if pts_anchors.len() == samples.len() {
        return samples
            .iter()
            .enumerate()
            .map(|(index, sample)| {
                let current_pts = pts_anchors[index].pts_90k;
                let duration = if let Some(next_anchor) = pts_anchors.get(index + 1) {
                    if next_anchor.pts_90k <= current_pts {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec: spec.to_string(),
                            message:
                                "transport-stream MPEG-4 Part 2 video carried non-monotonic PES timestamps across sample boundaries".to_string(),
                        });
                    }
                    u32::try_from(next_anchor.pts_90k - current_pts).map_err(|_| {
                        MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 duration")
                    })?
                } else {
                    u32::try_from(transport_mp4v_fallback_time(
                        i64::from(sample.duration),
                        fallback_base_duration,
                        spec,
                    )?)
                    .map_err(|_| {
                        MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 duration")
                    })?
                };
                Ok(CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration,
                    composition_time_offset: i32::try_from(transport_mp4v_fallback_time(
                        i64::from(sample.composition_time_offset),
                        fallback_base_duration,
                        spec,
                    )?)
                    .map_err(|_| {
                        MuxError::LayoutOverflow(
                            "transport-stream MPEG-4 Part 2 composition offset",
                        )
                    })?,
                    is_sync_sample: sample.is_sync_sample,
                })
            })
            .collect();
    }

    fallback_transport_mp4v_samples(spec, fallback_base_duration, &samples)
}

fn transport_mp4v_total_duration_override(samples: &[CandidateSample]) -> Option<u64> {
    let total_decode_duration = samples.iter().try_fold(0_u64, |total, sample| {
        total.checked_add(u64::from(sample.duration))
    })?;
    let max_positive_composition_offset = samples
        .iter()
        .filter_map(|sample| {
            (sample.composition_time_offset > 0).then_some(sample.composition_time_offset as u64)
        })
        .max()
        .unwrap_or(0);
    total_decode_duration.checked_add(max_positive_composition_offset)
}

fn transport_mp4v_fallback_time(
    value: i64,
    fallback_base_duration: u32,
    spec: &str,
) -> Result<i64, MuxError> {
    if fallback_base_duration == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream MPEG-4 Part 2 video did not expose a valid fallback frame cadence"
                    .to_string(),
        });
    }

    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(TRANSPORT_MP4V_FALLBACK_SAMPLE_DURATION))
        .ok_or(MuxError::LayoutOverflow(
            "transport-stream MPEG-4 Part 2 fallback time rescale",
        ))?;
    if scaled % u64::from(fallback_base_duration) != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream MPEG-4 Part 2 cadence does not map cleanly onto the retained 30 fps transport fallback".to_string(),
        });
    }
    let normalized = scaled / u64::from(fallback_base_duration);
    let normalized = i64::try_from(normalized).map_err(|_| {
        MuxError::LayoutOverflow("transport-stream MPEG-4 Part 2 fallback time rescale")
    })?;
    Ok(normalized * sign)
}

#[cfg(feature = "async")]
async fn finalize_transport_ac3_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<FinalizedTransportTrack, MuxError> {
    let parsed =
        scan_ac3_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let (timescale, samples, flat_chunk_sample_counts) =
        build_transport_ac3_packet_anchored_samples(
            spec,
            parsed.samples,
            parsed.sample_rate,
            &builder.pts_anchors,
        )?;
    let sample_entry_box = build_ac3_sample_entry_box_with_btrt(
        &parsed.decoder_config,
        timescale,
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
    )?;
    Ok(FinalizedTransportTrack {
        composite_track: CompositeTrackCandidate {
            track: TrackCandidate {
                track_id: u32::from(builder.pid),
                kind: MuxTrackKind::Audio,
                timescale,
                language: *b"und",
                handler_name: direct_ingest_handler_name("ac3"),
                mux_policy: direct_ingest_mux_policy("ac3", MuxTrackKind::Audio),
                width: 0,
                height: 0,
                sample_entry_box,
                source_edit_media_time: None,
                samples,
            },
            source_spec: SegmentedMuxSourceSpec {
                path: path.to_path_buf(),
                segments: builder.segments,
                total_size: builder.total_size,
            },
        },
        flat_chunk_sample_counts,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_truehd_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_truehd_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream TrueHD audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("truehd"),
            mux_policy: direct_ingest_mux_policy("truehd", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: build_truehd_sample_entry_box_with_btrt(
                parsed.descriptor,
                build_btrt_from_sample_sizes(
                    samples
                        .iter()
                        .map(|sample| (sample.data_size, sample.duration)),
                    timescale,
                )?,
            )?,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_eac3_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_eac3_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let (timescale, samples) = build_transport_timestamped_audio_samples(
        spec,
        "transport-stream E-AC-3 audio",
        parsed.samples,
        parsed.sample_rate,
        &builder.pts_anchors,
    )?;
    let rebuilt_sample_entry_box = build_eac3_sample_entry_box_with_btrt(
        &parsed.decoder_config,
        build_btrt_from_sample_sizes(
            samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            timescale,
        )?,
    )?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("eac3"),
            mux_policy: direct_ingest_mux_policy("eac3", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: rebuilt_sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_ac4_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_ac4_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale: parsed.media_time_scale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("ac4"),
            mux_policy: direct_ingest_mux_policy("ac4", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples: parsed
                .samples
                .into_iter()
                .map(|sample| CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration: sample.duration,
                    composition_time_offset: sample.composition_time_offset,
                    is_sync_sample: sample.is_sync_sample,
                })
                .collect(),
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_dts_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        scan_dts_segmented_async(file, &builder.segments, builder.total_size, spec).await?;
    let sample_entry_box = retune_carried_dts_sample_entry_box(&parsed.sample_entry_box)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Audio,
            timescale: parsed.media_timescale,
            language: *b"und",
            handler_name: direct_ingest_handler_name("dts"),
            mux_policy: direct_ingest_mux_policy("dts", MuxTrackKind::Audio),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples: parsed
                .samples
                .into_iter()
                .map(|sample| CandidateSample {
                    source_index: usize::MAX,
                    data_offset: sample.data_offset,
                    data_size: sample.data_size,
                    duration: sample.duration,
                    composition_time_offset: sample.composition_time_offset,
                    is_sync_sample: sample.is_sync_sample,
                })
                .collect(),
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_h264_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_h264_segmented_async(path, file, &builder.segments, builder.total_size, spec)
            .await?;
    let mut samples =
        rescale_transport_h26x_samples(parsed.samples, parsed.timescale, spec, "H.264")?;
    let mut source_edit_media_time = rescale_transport_h26x_edit_media_time(
        parsed.source_edit_media_time,
        parsed.timescale,
        spec,
        "H.264",
    )?;
    if source_edit_media_time.unwrap_or(0) != 0
        || samples
            .iter()
            .any(|sample| sample.composition_time_offset != 0)
    {
        align_transport_h264_presentation_time(
            &mut samples,
            &mut source_edit_media_time,
            &builder.pts_anchors,
        )?;
        normalize_transport_h264_wraparound_samples(&mut samples, &mut source_edit_media_time)?;
    } else {
        source_edit_media_time = None;
    }
    let sample_entry_box = retune_carried_h264_sample_entry_box(
        &parsed.sample_entry_box,
        TRANSPORT_VIDEO_TIMESCALE,
        Some(authored_h264_media_duration(samples.iter().map(
            |sample| (sample.duration, sample.composition_time_offset),
        ))?),
        samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        true,
        transport_h264_sample_entry_has_colr(&parsed.sample_entry_box)?,
    )?;
    let sample_entry_box = ensure_transport_h264_colorized_sample_entry(&sample_entry_box)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("h264"),
            mux_policy: direct_ingest_mux_policy("h264", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box,
            source_edit_media_time,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_h265_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_h265_segmented_async(path, file, &builder.segments, builder.total_size, spec)
            .await?;
    let samples = rescale_transport_h26x_samples(parsed.samples, parsed.timescale, spec, "H.265")?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("h265"),
            mux_policy: direct_ingest_mux_policy("h265", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: rescale_transport_h26x_edit_media_time(
                parsed.source_edit_media_time,
                parsed.timescale,
                spec,
                "H.265",
            )?,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_vvc_track_async(
    path: &Path,
    spec: &str,
    file: &mut TokioFile,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let parsed =
        stage_annex_b_vvc_segmented_async(path, file, &builder.segments, builder.total_size, spec)
            .await?;
    let samples = build_transport_vvc_samples(spec, parsed.samples, &builder.pts_anchors)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Video,
            timescale: TRANSPORT_VIDEO_TIMESCALE,
            language: *b"und",
            handler_name: direct_ingest_handler_name("vvc"),
            mux_policy: direct_ingest_mux_policy("vvc", MuxTrackKind::Video),
            width: parsed.track_width,
            height: parsed.track_height,
            sample_entry_box: parsed.sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: parsed.segmented_source,
    })
}

fn build_transport_full_au_samples(
    spec: &str,
    builder: &TransportTrackBuilder,
) -> Result<Vec<CandidateSample>, MuxError> {
    if builder.sample_offsets.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport stream input did not contain any subtitle or teletext PES payload units"
                    .to_string(),
        });
    }
    let mut samples = Vec::with_capacity(builder.sample_offsets.len());
    for (index, &sample_offset) in builder.sample_offsets.iter().enumerate() {
        let next_offset = builder
            .sample_offsets
            .get(index + 1)
            .copied()
            .unwrap_or(builder.total_size);
        if next_offset <= sample_offset {
            return Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "transport stream carried subtitle or teletext samples must advance monotonically"
                        .to_string(),
            });
        }
        let data_size = u32::try_from(next_offset - sample_offset).map_err(|_| {
            MuxError::LayoutOverflow("transport-stream carried subtitle sample size")
        })?;
        samples.push(CandidateSample {
            source_index: usize::MAX,
            data_offset: sample_offset,
            data_size,
            duration: DIRECT_SUBTITLE_SAMPLE_DURATION,
            composition_time_offset: 0,
            is_sync_sample: true,
        });
    }
    Ok(samples)
}

fn build_dvb_subtitle_sample_entry_box(
    spec: &str,
    builder: &TransportTrackBuilder,
) -> Result<Vec<u8>, MuxError> {
    let config = builder
        .dvb_subtitle
        .ok_or(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message:
                "transport-stream DVB subtitle builder is missing its descriptor configuration"
                    .to_string(),
        })?;
    let child_box = super::super::mp4::encode_typed_box(
        &DvsC {
            composition_page_id: config.composition_page_id,
            ancillary_page_id: config.ancillary_page_id,
            subtitle_type: config.subtitle_type,
        },
        &[],
    )?;
    build_generic_media_sample_entry_box(crate::FourCc::from_bytes(*b"dvbs"), &[child_box])
}

fn build_dvb_teletext_sample_entry_box() -> Result<Vec<u8>, MuxError> {
    build_generic_media_sample_entry_box(crate::FourCc::from_bytes(*b"dvbt"), &[])
}

fn finalize_transport_dvb_subtitle_track_sync(
    path: &Path,
    spec: &str,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let language = builder
        .dvb_subtitle
        .map(|config| config.language)
        .unwrap_or(builder.language);
    let sample_entry_box = build_dvb_subtitle_sample_entry_box(spec, &builder)?;
    let samples = build_transport_full_au_samples(spec, &builder)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Subtitle,
            timescale: DIRECT_SUBTITLE_TIMESCALE,
            language,
            handler_name: "SubtitleHandler".to_string(),
            mux_policy: direct_ingest_mux_policy("dvb-subtitle", MuxTrackKind::Subtitle),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

fn finalize_transport_dvb_teletext_track_sync(
    path: &Path,
    spec: &str,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    let sample_entry_box = build_dvb_teletext_sample_entry_box()?;
    let samples = build_transport_full_au_samples(spec, &builder)?;
    Ok(CompositeTrackCandidate {
        track: TrackCandidate {
            track_id: u32::from(builder.pid),
            kind: MuxTrackKind::Subtitle,
            timescale: DIRECT_SUBTITLE_TIMESCALE,
            language: builder.language,
            handler_name: "SubtitleHandler".to_string(),
            mux_policy: direct_ingest_mux_policy("dvb-teletext", MuxTrackKind::Subtitle),
            width: 0,
            height: 0,
            sample_entry_box,
            source_edit_media_time: None,
            samples,
        },
        source_spec: SegmentedMuxSourceSpec {
            path: path.to_path_buf(),
            segments: builder.segments,
            total_size: builder.total_size,
        },
    })
}

#[cfg(feature = "async")]
async fn finalize_transport_dvb_subtitle_track_async(
    path: &Path,
    spec: &str,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    finalize_transport_dvb_subtitle_track_sync(path, spec, 0, builder)
}

#[cfg(feature = "async")]
async fn finalize_transport_dvb_teletext_track_async(
    path: &Path,
    spec: &str,
    _track_index: usize,
    builder: TransportTrackBuilder,
) -> Result<CompositeTrackCandidate, MuxError> {
    finalize_transport_dvb_teletext_track_sync(path, spec, 0, builder)
}

#[cfg(test)]
mod tests {
    use super::{
        CandidateSample, TRANSPORT_MAX_PCR_90K, TransportTimestampAnchor,
        align_transport_h264_presentation_time,
    };

    #[test]
    fn align_transport_h264_presentation_time_normalizes_pts_wrap_delta() {
        let mut samples = vec![CandidateSample {
            source_index: 0,
            data_offset: 100,
            data_size: 10,
            duration: 3_003,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];
        let mut source_edit_media_time = Some(TRANSPORT_MAX_PCR_90K - 9_009);
        let pts_anchors = vec![TransportTimestampAnchor {
            sample_offset: 100,
            pts_90k: 9_009,
        }];

        align_transport_h264_presentation_time(
            &mut samples,
            &mut source_edit_media_time,
            &pts_anchors,
        )
        .unwrap();

        assert_eq!(samples[0].composition_time_offset, 18_018);
        assert_eq!(source_edit_media_time, Some(9_009));
    }

    #[test]
    fn align_transport_h264_presentation_time_shifts_negative_offsets_positive() {
        let mut samples = vec![CandidateSample {
            source_index: 0,
            data_offset: 100,
            data_size: 10,
            duration: 3_003,
            composition_time_offset: -2_167,
            is_sync_sample: true,
        }];
        let mut source_edit_media_time = Some(1_433);
        let pts_anchors = vec![TransportTimestampAnchor {
            sample_offset: 100,
            pts_90k: 1_433,
        }];

        align_transport_h264_presentation_time(
            &mut samples,
            &mut source_edit_media_time,
            &pts_anchors,
        )
        .unwrap();

        assert_eq!(samples[0].composition_time_offset, 0);
        assert_eq!(source_edit_media_time, Some(3_600));
    }
}
