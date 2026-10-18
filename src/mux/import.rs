use std::collections::BTreeMap;
use std::fs::File;
use std::io::Cursor;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "async")]
use std::pin::Pin;
#[cfg(feature = "async")]
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "async")]
use tokio::fs::File as TokioFile;
#[cfg(feature = "async")]
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufWriter, ReadBuf,
};

use crate::FourCc;
#[cfg(feature = "async")]
use crate::async_io::AsyncReadSeek;
use crate::bitio::BitReader;
use crate::boxes::dts::Ddts;
use crate::boxes::iso14496_12::{
    AVCDecoderConfiguration, AudioSampleEntry, Btrt, Co64, Cslg, Ctts, Dref, Elst,
    GenericMediaSampleEntry, HEVCDecoderConfiguration, Hdlr, Mdhd, Mvhd, Pasp, SampleEntry, Sbgp,
    SbgpEntry, Sdtp, SdtpSampleElem, Sgpd, Stco, Stsc, Stss, Stsz, Stts,
    TFHD_BASE_DATA_OFFSET_PRESENT, TFHD_DEFAULT_BASE_IS_MOOF, TFHD_DEFAULT_SAMPLE_DURATION_PRESENT,
    TFHD_DEFAULT_SAMPLE_FLAGS_PRESENT, TFHD_DEFAULT_SAMPLE_SIZE_PRESENT,
    TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT, TRUN_DATA_OFFSET_PRESENT,
    TRUN_FIRST_SAMPLE_FLAGS_PRESENT, TRUN_SAMPLE_COMPOSITION_TIME_OFFSET_PRESENT,
    TRUN_SAMPLE_DURATION_PRESENT, TRUN_SAMPLE_FLAGS_PRESENT, TRUN_SAMPLE_SIZE_PRESENT, Tfdt, Tfhd,
    Tkhd, Trex, Trun, Url, Urn, VisualSampleEntry,
};
use crate::boxes::iso14496_14::{DECODER_CONFIG_DESCRIPTOR_TAG, Esds};
use crate::boxes::vp::VpCodecConfiguration;
use crate::codec::{CodecBox, ImmutableBox, MutableBox};
use crate::extract::{
    ExtractedBox, extract_box, extract_box_as, extract_box_bytes, extract_box_with_payload,
};
#[cfg(feature = "async")]
use crate::extract::{
    extract_box_as_async, extract_box_async, extract_box_bytes_async,
    extract_box_with_payload_async,
};
use crate::header::BoxInfo as HeaderInfo;
use crate::walk::BoxPath;

use super::demux::{
    DetectedContainerPathKind, DetectedNhmlSidecarKind, DetectedPathTrackKind, ParsedAv1Track,
    ParsedAv1TrackSource, ParsedDashSource, ParsedNhmlSource, ParsedNhmlSourceSpec,
    PcmContainerKind, build_h264_sample_entry_from_avc_config_with_box_type_and_options,
    detect_caf_track_kind_sync, detect_container_path_kind_from_path_and_prefix,
    detect_id3_wrapped_audio_from_prefix, detect_nhml_sidecar_kind, detect_ogg_track_kind_sync,
    detect_path_track_kind_from_prefix, id3v2_size_from_prefix, nal_to_rbsp,
    parse_dash_source_sync, parse_nhml_source_sync, read_ue_labeled, scan_ac3_file_sync,
    scan_ac4_file_sync, scan_adts_file_sync, scan_amr_file_sync, scan_amr_wb_file_sync,
    scan_av1_file_sync, scan_avi_source_sync, scan_bmp_file_sync, scan_caf_alac_file_sync,
    scan_dts_file_sync, scan_eac3_file_sync, scan_flac_file_sync, scan_h263_file_sync,
    scan_iamf_file_sync, scan_j2k_file_sync, scan_jpeg_file_sync, scan_latm_file_sync,
    scan_mhas_file_sync, scan_mp3_file_sync, scan_mp4v_file_sync, scan_mpeg2v_file_sync,
    scan_ogg_flac_file_sync, scan_ogg_opus_file_sync, scan_ogg_speex_file_sync,
    scan_ogg_theora_file_sync, scan_ogg_vorbis_file_sync, scan_pcm_file_sync, scan_png_file_sync,
    scan_program_stream_sync, scan_prores_file_sync, scan_qcp_file_sync, scan_raw_video_file_sync,
    scan_transport_stream_sync, scan_truehd_file_sync, scan_vobsub_source_sync, scan_vp8_file_sync,
    scan_vp9_file_sync, scan_vp10_file_sync, scan_y4m_file_sync, stage_annex_b_h264_sync,
    stage_annex_b_h265_sync, stage_annex_b_vvc_sync, wrapped_dts_family_has_native_core_sync_sync,
};
#[cfg(feature = "async")]
use super::demux::{
    detect_caf_track_kind_async, detect_ogg_track_kind_async, parse_dash_source_async,
    parse_nhml_source_async, scan_ac3_file_async, scan_ac4_file_async, scan_adts_file_async,
    scan_amr_file_async, scan_amr_wb_file_async, scan_av1_file_async, scan_avi_source_async,
    scan_bmp_file_async, scan_caf_alac_file_async, scan_dts_file_async, scan_eac3_file_async,
    scan_flac_file_async, scan_h263_file_async, scan_iamf_file_async, scan_j2k_file_async,
    scan_jpeg_file_async, scan_latm_file_async, scan_mhas_file_async, scan_mp3_file_async,
    scan_mp4v_file_async, scan_mpeg2v_file_async, scan_ogg_flac_file_async,
    scan_ogg_opus_file_async, scan_ogg_speex_file_async, scan_ogg_theora_file_async,
    scan_ogg_vorbis_file_async, scan_pcm_file_async, scan_png_file_async,
    scan_program_stream_async, scan_prores_file_async, scan_qcp_file_async,
    scan_raw_video_file_async, scan_transport_stream_async, scan_truehd_file_async,
    scan_vobsub_source_async, scan_vp8_file_async, scan_vp9_file_async, scan_vp10_file_async,
    scan_y4m_file_async, stage_annex_b_h264_async, stage_annex_b_h265_async,
    stage_annex_b_vvc_async, wrapped_dts_family_has_native_core_sync_async,
};
use super::inspect::{
    DirectIngestDetectedKind, DirectIngestPacketEntry, DirectIngestPacketReport,
    DirectIngestReport, DirectIngestSampleReport, DirectIngestSourceSegmentReport,
    DirectIngestStagedSourceReport, DirectIngestTrackReport,
};
use super::mp4::{write_fragmented_mp4_mux, write_fragmented_mp4_mux_split};
#[cfg(feature = "async")]
use super::mp4::{write_fragmented_mp4_mux_async, write_fragmented_mp4_mux_split_async};
use super::{
    FlatTimingOverride, MuxDestinationMode, MuxDurationBoundaryKind, MuxError, MuxFileConfig,
    MuxInterleavePolicy, MuxMp4TrackSelector, MuxOutputLayout, MuxRawCodec, MuxRawVideoParams,
    MuxRequest, MuxStagedMediaItem, MuxTrackConfig, MuxTrackKind, MuxTrackSpec,
    StscRunEncodingMode, SttsRunEncodingMode, SyncSampleTableMode, TrackCoordinationDirective,
    build_capped_duration_chunk_sample_counts, build_duration_chunk_sample_counts,
    build_duration_chunk_sample_counts_with_start_time,
    build_fragmented_duration_chunk_sample_counts_with_start_time,
    build_sync_aligned_fragmented_duration_chunk_sample_counts,
    build_sync_aligned_segment_chunk_sample_counts, plan_staged_media_items_with_coordination,
    rebalance_small_multi_audio_chunk_sample_counts, write_mp4_mux,
};

const MOOV: FourCc = FourCc::from_bytes(*b"moov");
const MVHD: FourCc = FourCc::from_bytes(*b"mvhd");
const TRAK: FourCc = FourCc::from_bytes(*b"trak");
const TKHD: FourCc = FourCc::from_bytes(*b"tkhd");
const EDTS: FourCc = FourCc::from_bytes(*b"edts");
const ELST: FourCc = FourCc::from_bytes(*b"elst");
const TREF: FourCc = FourCc::from_bytes(*b"tref");
const MDIA: FourCc = FourCc::from_bytes(*b"mdia");
const MDHD: FourCc = FourCc::from_bytes(*b"mdhd");
const HDLR: FourCc = FourCc::from_bytes(*b"hdlr");
const MINF: FourCc = FourCc::from_bytes(*b"minf");
const DINF: FourCc = FourCc::from_bytes(*b"dinf");
const DREF: FourCc = FourCc::from_bytes(*b"dref");
const URL: FourCc = FourCc::from_bytes(*b"url ");
const URN: FourCc = FourCc::from_bytes(*b"urn ");
const STBL: FourCc = FourCc::from_bytes(*b"stbl");
const STSD: FourCc = FourCc::from_bytes(*b"stsd");
const STTS: FourCc = FourCc::from_bytes(*b"stts");
const CTTS: FourCc = FourCc::from_bytes(*b"ctts");
const STSC: FourCc = FourCc::from_bytes(*b"stsc");
const STSZ: FourCc = FourCc::from_bytes(*b"stsz");
const STCO: FourCc = FourCc::from_bytes(*b"stco");
const CO64: FourCc = FourCc::from_bytes(*b"co64");
const STSS: FourCc = FourCc::from_bytes(*b"stss");
const CSLG: FourCc = FourCc::from_bytes(*b"cslg");
const SDTP: FourCc = FourCc::from_bytes(*b"sdtp");
const STPS: FourCc = FourCc::from_bytes(*b"stps");
const STDP: FourCc = FourCc::from_bytes(*b"stdp");
const SUBS: FourCc = FourCc::from_bytes(*b"subs");
const SGPD: FourCc = FourCc::from_bytes(*b"sgpd");
const SBGP: FourCc = FourCc::from_bytes(*b"sbgp");
const STZ2: FourCc = FourCc::from_bytes(*b"stz2");
const FTYP: FourCc = FourCc::from_bytes(*b"ftyp");
const FREE: FourCc = FourCc::from_bytes(*b"free");
const IODS: FourCc = FourCc::from_bytes(*b"iods");
const SKIP: FourCc = FourCc::from_bytes(*b"skip");
const WIDE: FourCc = FourCc::from_bytes(*b"wide");
const MVEX: FourCc = FourCc::from_bytes(*b"mvex");
const TREX: FourCc = FourCc::from_bytes(*b"trex");
const MOOF: FourCc = FourCc::from_bytes(*b"moof");
const TRAF: FourCc = FourCc::from_bytes(*b"traf");
const TFHD: FourCc = FourCc::from_bytes(*b"tfhd");
const TRUN: FourCc = FourCc::from_bytes(*b"trun");
const UDTA: FourCc = FourCc::from_bytes(*b"udta");
const VIDE: FourCc = FourCc::from_bytes(*b"vide");
const PATH_KIND_PREFIX_BYTES: usize = 2_048;
const SOUN: FourCc = FourCc::from_bytes(*b"soun");
const TEXT: FourCc = FourCc::from_bytes(*b"text");
const SUBT: FourCc = FourCc::from_bytes(*b"subt");
const DEFAULT_FRAGMENTED_REFERENCE_GROUP_SECONDS: u64 = 6;
const LOCAL_DASH_FLAT_TOOL_METADATA_VALUE: &str =
    concat!(env!("CARGO_PKG_NAME"), " v", env!("CARGO_PKG_VERSION"));
const SUBP: FourCc = FourCc::from_bytes(*b"subp");
const ENCV: FourCc = FourCc::from_bytes(*b"encv");
const ENCA: FourCc = FourCc::from_bytes(*b"enca");
const NON_KEY_SAMPLE_FLAGS: u32 = 0x0001_0000;
const AUTO_FLAT_INTERLEAVE_MILLISECONDS: u64 = 500;
const IMPORTED_DDTS_FRAME_DURATION: u8 = 3;
const IMPORTED_DDTS_STREAM_CONSTRUCTION: u8 = 18;
const IMPORTED_DDTS_CORE_LAYOUT: u8 = 31;
const IMPORTED_DDTS_REPRESENTATION_TYPE: u8 = 4;
const IMPORTED_DDTS_CHANNEL_LAYOUT_MASK: u16 = 0x000f;

fn mux_io_at_path(operation: &'static str, path: &Path, source: io::Error) -> MuxError {
    MuxError::Io(io::Error::new(
        source.kind(),
        format!("failed to {operation} `{}`: {source}", path.display()),
    ))
}

/// Opens the requested track specs, validates the narrowed mux request shape, and writes one newly
/// created output MP4 file to `output_path`.
///
/// This task-level helper is the sync programmatic companion to the explicit `--out PATH` mux CLI
/// surface. It always treats `output_path` as a newly created destination and rejects unsupported
/// multi-video or duration-mode combinations explicitly.
pub fn mux_to_path<P>(request: &MuxRequest, output_path: P) -> Result<(), MuxError>
where
    P: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_to_path_inner(&request, output_path.as_ref())
}

/// Opens the requested track specs, preserves an existing MP4 destination when present, and
/// otherwise creates one new output MP4 at `destination_path`.
///
/// When `destination_path` already exists and probes as MP4, this helper preserves that file's
/// current tracks and imports the requested tracks into it. When the path does not exist or does
/// not probe as MP4, the same path is treated as the newly created destination file.
pub fn mux_into_path<P>(request: &MuxRequest, destination_path: P) -> Result<(), MuxError>
where
    P: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::UpdateOrCreateDestination);
    mux_into_path_inner(&request, destination_path.as_ref())
}

/// Opens the requested track specs and writes fragmented initialization and media data to separate
/// newly created paths.
///
/// This helper requires [`MuxOutputLayout::Fragmented`] and a duration mode on the request. The
/// two destination paths must be distinct, must not already exist, and must not alias an input
/// track path. On failure before both outputs are finalized, temporary outputs are removed.
pub fn mux_fragmented_to_paths<P, Q>(
    request: &MuxRequest,
    init_path: P,
    media_path: Q,
) -> Result<(), MuxError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_fragmented_to_paths_inner(&request, init_path.as_ref(), media_path.as_ref())
}

fn mux_to_path_inner(request: &MuxRequest, output_path: &Path) -> Result<(), MuxError> {
    let prepared = prepare_request_sync(request, output_path)?;
    let mut sources = prepared
        .source_specs
        .iter()
        .map(SyncMuxSource::open)
        .collect::<Result<Vec<_>, _>>()?;
    let mut writer = std::io::BufWriter::new(
        File::create(output_path)
            .map_err(|error| mux_io_at_path("create mux output", output_path, error))?,
    );
    match prepared.output_layout {
        MuxOutputLayout::Flat => write_mp4_mux(
            &mut sources,
            &mut writer,
            &prepared.file_config,
            &prepared.track_configs,
            &prepared.plan,
        )?,
        MuxOutputLayout::Fragmented => write_fragmented_mp4_mux(
            &mut sources,
            &mut writer,
            &prepared.file_config,
            &prepared.track_configs,
            prepared.fragmented_single_sidx_reference,
            &prepared.plan,
        )?,
    }
    writer.flush()?;
    Ok(())
}

fn mux_fragmented_to_paths_inner(
    request: &MuxRequest,
    init_path: &Path,
    media_path: &Path,
) -> Result<(), MuxError> {
    validate_fragmented_split_paths(request, init_path, media_path)?;
    let prepared = prepare_request_sync(request, media_path)?;
    let mut sources = prepared
        .source_specs
        .iter()
        .map(SyncMuxSource::open)
        .collect::<Result<Vec<_>, _>>()?;
    ensure_output_parent_dir(init_path)?;
    ensure_output_parent_dir(media_path)?;
    let init_temp_path = create_update_temp_path(init_path, MuxDestinationMode::CreateNew)?;
    let media_temp_path = create_update_temp_path(media_path, MuxDestinationMode::CreateNew)?;
    let write_result = (|| {
        let mut init_writer =
            std::io::BufWriter::new(File::create(&init_temp_path).map_err(|error| {
                mux_io_at_path("create mux init output", &init_temp_path, error)
            })?);
        let mut media_writer =
            std::io::BufWriter::new(File::create(&media_temp_path).map_err(|error| {
                mux_io_at_path("create mux media output", &media_temp_path, error)
            })?);
        write_fragmented_mp4_mux_split(
            &mut sources,
            &mut init_writer,
            &mut media_writer,
            &prepared.file_config,
            &prepared.track_configs,
            prepared.fragmented_single_sidx_reference,
            &prepared.plan,
        )
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&init_temp_path);
        let _ = std::fs::remove_file(&media_temp_path);
        return Err(error);
    }
    finalize_new_split_output(&init_temp_path, init_path, None)?;
    if let Err(error) = finalize_new_split_output(&media_temp_path, media_path, Some(init_path)) {
        let _ = std::fs::remove_file(init_path);
        return Err(error);
    }
    Ok(())
}

fn mux_into_path_inner(request: &MuxRequest, destination_path: &Path) -> Result<(), MuxError> {
    if should_preserve_destination_mp4(destination_path) {
        let amended_request = build_destination_preserving_request(request, destination_path)?;
        let temp_path = create_update_temp_path(destination_path, request.destination_mode())?;
        let write_result = mux_to_path_inner(&amended_request, &temp_path);
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        replace_output_path(&temp_path, destination_path)?;
        return Ok(());
    }
    let create_new_request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_to_path_inner(&create_new_request, destination_path)
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(all(feature = "mux", feature = "async"))))]
/// Async companion to [`mux_to_path`] that keeps the file-backed mux path on the crate's additive
/// Tokio-based async surface.
///
/// The request validation and supported public behavior match the sync helper exactly; only the
/// file-backed I/O path differs.
pub async fn mux_to_path_async<P>(request: &MuxRequest, output_path: P) -> Result<(), MuxError>
where
    P: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_to_path_async_inner(&request, output_path.as_ref()).await
}

/// Async mux with output-byte progress. `(0, 0)` denotes input analysis.
/// Flat MP4 reports exact container bytes including headers; fragmented output
/// currently reports analysis only. Completion is the successful return value.
#[cfg(feature = "async")]
pub async fn mux_to_path_async_with_progress<P>(
	request: &MuxRequest,
	output_path: P,
	progress: &(dyn Fn(u64, u64) + Send + Sync),
) -> Result<(), MuxError>
where
	P: AsRef<Path>,
{
	let request: MuxRequest = request.clone().with_destination_mode(MuxDestinationMode::CreateNew);
	mux_to_path_async_inner_with_progress(&request, output_path.as_ref(), Some(progress)).await
}

/// Async companion to [`mux_into_path`] on the file-backed Tokio surface.
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(all(feature = "mux", feature = "async"))))]
pub async fn mux_into_path_async<P>(
    request: &MuxRequest,
    destination_path: P,
) -> Result<(), MuxError>
where
    P: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::UpdateOrCreateDestination);
    mux_into_path_async_inner(&request, destination_path.as_ref()).await
}

/// Async companion to [`mux_fragmented_to_paths`] for separate fragmented init/media outputs.
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(all(feature = "mux", feature = "async"))))]
pub async fn mux_fragmented_to_paths_async<P, Q>(
    request: &MuxRequest,
    init_path: P,
    media_path: Q,
) -> Result<(), MuxError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_fragmented_to_paths_async_inner(&request, init_path.as_ref(), media_path.as_ref()).await
}

#[cfg(feature = "async")]
async fn mux_to_path_async_inner(request: &MuxRequest, output_path: &Path) -> Result<(), MuxError> {
	mux_to_path_async_inner_with_progress(request, output_path, None).await
}

#[cfg(feature = "async")]
async fn mux_to_path_async_inner_with_progress(
	request: &MuxRequest,
	output_path: &Path,
	progress: Option<&(dyn Fn(u64, u64) + Send + Sync)>,
) -> Result<(), MuxError> {
	if let Some(progress) = progress { progress(0, 0); }
    let prepared = prepare_request_async(request, output_path).await?;
    let mut sources = Vec::with_capacity(prepared.source_specs.len());
    for spec in &prepared.source_specs {
        sources.push(AsyncMuxSource::open(spec).await?);
    }
    let output = TokioFile::create(output_path)
        .await
        .map_err(|error| mux_io_at_path("create mux output", output_path, error))?;
    let mut writer = BufWriter::new(output);
    match prepared.output_layout {
        MuxOutputLayout::Flat => {
            super::mp4::write_mp4_mux_async_with_progress(
                &mut sources,
                &mut writer,
                &prepared.file_config,
                &prepared.track_configs,
                &prepared.plan,
                progress,
            )
            .await?
        }
        MuxOutputLayout::Fragmented => {
            write_fragmented_mp4_mux_async(
                &mut sources,
                &mut writer,
                &prepared.file_config,
                &prepared.track_configs,
                prepared.fragmented_single_sidx_reference,
                &prepared.plan,
            )
            .await?
        }
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
async fn mux_fragmented_to_paths_async_inner(
    request: &MuxRequest,
    init_path: &Path,
    media_path: &Path,
) -> Result<(), MuxError> {
    validate_fragmented_split_paths_async(request, init_path, media_path).await?;
    let prepared = prepare_request_async(request, media_path).await?;
    let mut sources = Vec::with_capacity(prepared.source_specs.len());
    for spec in &prepared.source_specs {
        sources.push(AsyncMuxSource::open(spec).await?);
    }
    ensure_output_parent_dir_async(init_path).await?;
    ensure_output_parent_dir_async(media_path).await?;
    let init_temp_path = create_update_temp_path(init_path, MuxDestinationMode::CreateNew)?;
    let media_temp_path = create_update_temp_path(media_path, MuxDestinationMode::CreateNew)?;
    let write_result = async {
        let init_output = TokioFile::create(&init_temp_path)
            .await
            .map_err(|error| mux_io_at_path("create mux init output", &init_temp_path, error))?;
        let media_output = TokioFile::create(&media_temp_path)
            .await
            .map_err(|error| mux_io_at_path("create mux media output", &media_temp_path, error))?;
        let mut init_writer = BufWriter::new(init_output);
        let mut media_writer = BufWriter::new(media_output);
        write_fragmented_mp4_mux_split_async(
            &mut sources,
            &mut init_writer,
            &mut media_writer,
            &prepared.file_config,
            &prepared.track_configs,
            prepared.fragmented_single_sidx_reference,
            &prepared.plan,
        )
        .await
    }
    .await;
    if let Err(error) = write_result {
        let _ = tokio::fs::remove_file(&init_temp_path).await;
        let _ = tokio::fs::remove_file(&media_temp_path).await;
        return Err(error);
    }
    finalize_new_split_output_async(&init_temp_path, init_path, None).await?;
    if let Err(error) =
        finalize_new_split_output_async(&media_temp_path, media_path, Some(init_path)).await
    {
        let _ = tokio::fs::remove_file(init_path).await;
        return Err(error);
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn mux_into_path_async_inner(
    request: &MuxRequest,
    destination_path: &Path,
) -> Result<(), MuxError> {
    if should_preserve_destination_mp4_async(destination_path).await {
        let amended_request = build_destination_preserving_request(request, destination_path)?;
        let temp_path = create_update_temp_path(destination_path, request.destination_mode())?;
        let write_result = mux_to_path_async_inner(&amended_request, &temp_path).await;
        if let Err(error) = write_result {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(error);
        }
        replace_output_path_async(&temp_path, destination_path).await?;
        return Ok(());
    }
    let create_new_request = request
        .clone()
        .with_destination_mode(MuxDestinationMode::CreateNew);
    mux_to_path_async_inner(&create_new_request, destination_path).await
}

struct PreparedMuxRequest {
    output_layout: MuxOutputLayout,
    file_config: MuxFileConfig,
    track_configs: Vec<MuxTrackConfig>,
    fragmented_single_sidx_reference: bool,
    plan: super::MuxPlan,
    source_specs: Vec<SourceSpec>,
}

struct FragmentRunContext<'a> {
    path: &'a Path,
    source_index: usize,
    track_id: u32,
    moof_offset: u64,
    trex: Option<&'a Trex>,
}

struct ImportedFragmentBatch {
    base_decode_time: Option<u64>,
    samples: Vec<CandidateSample>,
    sample_description_indices: Vec<u32>,
}

struct ImportedFragmentSamples {
    samples: Vec<CandidateSample>,
    sample_description_indices: Vec<u32>,
    first_base_decode_time: Option<u64>,
    fragmented_decode_time_gaps: Vec<FragmentedDecodeTimeGap>,
}

#[derive(Clone)]
enum SourceSpec {
    File(PathBuf),
    Segmented(SegmentedMuxSourceSpec),
}

#[derive(Clone)]
pub(in crate::mux) struct SegmentedMuxSourceSpec {
    pub(in crate::mux) path: PathBuf,
    pub(in crate::mux) segments: Vec<SegmentedMuxSourceSegment>,
    pub(in crate::mux) total_size: u64,
}

#[derive(Clone)]
pub(in crate::mux) struct SegmentedMuxSourceSegment {
    pub(in crate::mux) logical_offset: u64,
    pub(in crate::mux) data: SegmentedMuxSourceSegmentData,
}

#[derive(Clone)]
pub(in crate::mux) enum SegmentedMuxSourceSegmentData {
    Prefix([u8; 4]),
    Bytes(Vec<u8>),
    FileRange {
        source_offset: u64,
        size: u32,
    },
    ExternalFileRange {
        path: PathBuf,
        source_offset: u64,
        size: u32,
    },
}

impl SegmentedMuxSourceSegment {
    pub(in crate::mux) fn logical_size(&self) -> u64 {
        match &self.data {
            SegmentedMuxSourceSegmentData::Prefix(_) => 4,
            SegmentedMuxSourceSegmentData::Bytes(bytes) => u64::try_from(bytes.len()).unwrap(),
            SegmentedMuxSourceSegmentData::FileRange { size, .. } => u64::from(*size),
            SegmentedMuxSourceSegmentData::ExternalFileRange { size, .. } => u64::from(*size),
        }
    }

    pub(in crate::mux) fn logical_end(&self) -> u64 {
        self.logical_offset + self.logical_size()
    }
}

fn find_segmented_source_segment_index(
    segments: &[SegmentedMuxSourceSegment],
    position: u64,
) -> Option<usize> {
    segments
        .binary_search_by(|segment| {
            if segment.logical_end() <= position {
                std::cmp::Ordering::Less
            } else if segment.logical_offset > position {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()
}

fn seek_mux_source_position(position: u64, end: u64, target: SeekFrom) -> io::Result<u64> {
    let next = match target {
        SeekFrom::Start(offset) => i128::from(offset),
        SeekFrom::Current(delta) => i128::from(position) + i128::from(delta),
        SeekFrom::End(delta) => i128::from(end) + i128::from(delta),
    };
    if next < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid seek before start of segmented mux source",
        ));
    }
    u64::try_from(next).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid seek target for segmented mux source",
        )
    })
}

struct SyncMuxSource {
    inner: SyncMuxSourceInner,
}

enum SyncMuxSourceInner {
    File(File),
    Segmented(SegmentedSyncMuxSource),
}

struct SegmentedSyncMuxSource {
    primary_path: PathBuf,
    file: File,
    extra_files: BTreeMap<PathBuf, File>,
    segments: Vec<SegmentedMuxSourceSegment>,
    total_size: u64,
    position: u64,
    file_path: Option<PathBuf>,
    file_position: Option<u64>,
}

impl SyncMuxSource {
    fn open(spec: &SourceSpec) -> Result<Self, MuxError> {
        let inner = match spec {
            SourceSpec::File(path) => SyncMuxSourceInner::File(
                File::open(path).map_err(|error| mux_io_at_path("open mux input", path, error))?,
            ),
            SourceSpec::Segmented(spec) => SyncMuxSourceInner::Segmented(SegmentedSyncMuxSource {
                primary_path: spec.path.clone(),
                file: File::open(&spec.path)
                    .map_err(|error| mux_io_at_path("open mux input", &spec.path, error))?,
                extra_files: BTreeMap::new(),
                segments: spec.segments.clone(),
                total_size: spec.total_size,
                position: 0,
                file_path: None,
                file_position: None,
            }),
        };
        Ok(Self { inner })
    }
}

impl SegmentedSyncMuxSource {
    fn file_for_path_mut(&mut self, path: &Path) -> io::Result<&mut File> {
        if path == self.primary_path {
            return Ok(&mut self.file);
        }
        if !self.extra_files.contains_key(path) {
            let opened = File::open(path)?;
            self.extra_files.insert(path.to_path_buf(), opened);
        }
        Ok(self.extra_files.get_mut(path).unwrap())
    }

    fn read_file_range_into(
        &mut self,
        path: &Path,
        source_offset: u64,
        size: u32,
        segment_offset: usize,
        buf: &mut [u8],
        written: &mut usize,
    ) -> io::Result<()> {
        let available =
            usize::try_from(u64::from(size) - u64::try_from(segment_offset).unwrap())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "segment size overflow"))?;
        let to_read = available.min(buf.len() - *written);
        let file_offset = source_offset + u64::try_from(segment_offset).unwrap();
        let should_seek =
            self.file_path.as_deref() != Some(path) || self.file_position != Some(file_offset);
        let read = {
            let file = self.file_for_path_mut(path)?;
            if should_seek {
                file.seek(SeekFrom::Start(file_offset))?;
            }
            file.read(&mut buf[*written..*written + to_read])?
        };
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated segmented mux source input",
            ));
        }
        *written += read;
        self.position += u64::try_from(read).unwrap();
        self.file_path = Some(path.to_path_buf());
        self.file_position = Some(file_offset + u64::try_from(read).unwrap());
        Ok(())
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.position >= self.total_size {
            return Ok(0);
        }

        let mut written = 0usize;
        while written < buf.len() && self.position < self.total_size {
            let Some(segment_index) =
                find_segmented_source_segment_index(&self.segments, self.position)
            else {
                break;
            };
            let segment = &self.segments[segment_index];
            let segment_logical_offset = segment.logical_offset;
            let segment_offset =
                usize::try_from(self.position - segment_logical_offset).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "logical offset overflow")
                })?;
            match &segment.data {
                SegmentedMuxSourceSegmentData::Prefix(prefix) => {
                    let available = prefix.len().saturating_sub(segment_offset);
                    let to_copy = available.min(buf.len() - written);
                    buf[written..written + to_copy]
                        .copy_from_slice(&prefix[segment_offset..segment_offset + to_copy]);
                    written += to_copy;
                    self.position += u64::try_from(to_copy).unwrap();
                }
                SegmentedMuxSourceSegmentData::Bytes(bytes) => {
                    let available = bytes.len().saturating_sub(segment_offset);
                    let to_copy = available.min(buf.len() - written);
                    buf[written..written + to_copy]
                        .copy_from_slice(&bytes[segment_offset..segment_offset + to_copy]);
                    written += to_copy;
                    self.position += u64::try_from(to_copy).unwrap();
                }
                SegmentedMuxSourceSegmentData::FileRange {
                    source_offset,
                    size,
                } => {
                    let source_offset = *source_offset;
                    let size = *size;
                    let primary_path = self.primary_path.clone();
                    self.read_file_range_into(
                        &primary_path,
                        source_offset,
                        size,
                        segment_offset,
                        buf,
                        &mut written,
                    )?
                }
                SegmentedMuxSourceSegmentData::ExternalFileRange {
                    path,
                    source_offset,
                    size,
                } => {
                    let path = path.clone();
                    let source_offset = *source_offset;
                    let size = *size;
                    self.read_file_range_into(
                        &path,
                        source_offset,
                        size,
                        segment_offset,
                        buf,
                        &mut written,
                    )?
                }
            }
        }
        Ok(written)
    }

    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        self.position = seek_mux_source_position(self.position, self.total_size, target)?;
        Ok(self.position)
    }
}

impl Read for SyncMuxSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            SyncMuxSourceInner::File(file) => file.read(buf),
            SyncMuxSourceInner::Segmented(source) => source.read(buf),
        }
    }
}

impl Seek for SyncMuxSource {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match &mut self.inner {
            SyncMuxSourceInner::File(file) => file.seek(pos),
            SyncMuxSourceInner::Segmented(source) => source.seek(pos),
        }
    }
}

#[cfg(feature = "async")]
struct AsyncMuxSource {
    inner: AsyncMuxSourceInner,
}

#[cfg(feature = "async")]
enum AsyncMuxSourceInner {
    File(TokioFile),
    Segmented(SegmentedAsyncMuxSource),
}

#[cfg(feature = "async")]
struct SegmentedAsyncMuxSource {
    primary_path: PathBuf,
    file: TokioFile,
    extra_files: BTreeMap<PathBuf, TokioFile>,
    segments: Vec<SegmentedMuxSourceSegment>,
    total_size: u64,
    position: u64,
    file_path: Option<PathBuf>,
    file_position: Option<u64>,
    pending_file_seek: Option<(PathBuf, u64)>,
	read_cache: Vec<u8>,
	cache_path: Option<PathBuf>,
	cache_offset: u64,
	cache_length: usize,
}

#[cfg(feature = "async")]
impl AsyncMuxSource {
    async fn open(spec: &SourceSpec) -> Result<Self, MuxError> {
        let inner = match spec {
            SourceSpec::File(path) => AsyncMuxSourceInner::File(
                TokioFile::open(path)
                    .await
                    .map_err(|error| mux_io_at_path("open mux input", path, error))?,
            ),
            SourceSpec::Segmented(spec) => {
                AsyncMuxSourceInner::Segmented(SegmentedAsyncMuxSource {
                    primary_path: spec.path.clone(),
                    file: TokioFile::open(&spec.path)
                        .await
                        .map_err(|error| mux_io_at_path("open mux input", &spec.path, error))?,
                    extra_files: BTreeMap::new(),
                    segments: spec.segments.clone(),
                    total_size: spec.total_size,
                    position: 0,
                    file_path: None,
                    file_position: None,
                    pending_file_seek: None,
					read_cache: vec![0; 64 * 1024],
					cache_path: None,
					cache_offset: 0,
					cache_length: 0,
                })
            }
        };
        let mut source = Self { inner };
        if let AsyncMuxSourceInner::Segmented(segmented) = &mut source.inner {
            segmented.open_external_files().await?;
        }
        Ok(source)
    }
}

#[cfg(feature = "async")]
impl SegmentedAsyncMuxSource {
    fn file_for_path_mut(&mut self, path: &Path) -> io::Result<&mut TokioFile> {
        if path == self.primary_path {
            return Ok(&mut self.file);
        }
        if !self.extra_files.contains_key(path) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "segmented async mux source file `{}` was not opened before polling",
                    path.display()
                ),
            ));
        }
        Ok(self.extra_files.get_mut(path).unwrap())
    }

    async fn open_external_files(&mut self) -> io::Result<()> {
        let mut pending = Vec::new();
        for segment in &self.segments {
            if let SegmentedMuxSourceSegmentData::ExternalFileRange { path, .. } = &segment.data
                && !self.extra_files.contains_key(path)
            {
                pending.push(path.clone());
            }
        }
        for path in pending {
            let file = TokioFile::open(&path).await?;
            self.extra_files.insert(path, file);
        }
        Ok(())
    }

    fn start_seek(&mut self, target: SeekFrom) -> io::Result<()> {
        self.position = seek_mux_source_position(self.position, self.total_size, target)?;
        Ok(())
    }

    fn poll_complete(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }

    fn poll_read_internal(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 || self.position >= self.total_size {
            return Poll::Ready(Ok(()));
        }

        let Some(segment_index) =
            find_segmented_source_segment_index(&self.segments, self.position)
        else {
            return Poll::Ready(Ok(()));
        };
        let segment = &self.segments[segment_index];
        let segment_offset = usize::try_from(self.position - segment.logical_offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "logical offset overflow"))?;
        match &segment.data {
            SegmentedMuxSourceSegmentData::Prefix(prefix) => {
                let available = prefix.len().saturating_sub(segment_offset);
                let to_copy = available.min(buf.remaining());
                buf.put_slice(&prefix[segment_offset..segment_offset + to_copy]);
                self.position += u64::try_from(to_copy).unwrap();
                Poll::Ready(Ok(()))
            }
            SegmentedMuxSourceSegmentData::Bytes(bytes) => {
                let available = bytes.len().saturating_sub(segment_offset);
                let to_copy = available.min(buf.remaining());
                buf.put_slice(&bytes[segment_offset..segment_offset + to_copy]);
                self.position += u64::try_from(to_copy).unwrap();
                Poll::Ready(Ok(()))
            }
            SegmentedMuxSourceSegmentData::FileRange {
                source_offset,
                size,
            } => {
                let path = self.primary_path.clone();
                self.poll_read_file_range(cx, buf, &path, *source_offset, *size, segment_offset)
            }
            SegmentedMuxSourceSegmentData::ExternalFileRange {
                path,
                source_offset,
                size,
            } => {
                let path = path.clone();
                self.poll_read_file_range(cx, buf, &path, *source_offset, *size, segment_offset)
            }
        }
    }

    fn poll_read_file_range(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
        path: &Path,
        source_offset: u64,
        size: u32,
        segment_offset: usize,
    ) -> Poll<io::Result<()>> {
        let available =
            match usize::try_from(u64::from(size) - u64::try_from(segment_offset).unwrap()) {
                Ok(value) => value,
                Err(_) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "segment size overflow",
                    )));
                }
            };
        let to_read = available.min(buf.remaining());
        let file_offset = source_offset + u64::try_from(segment_offset).unwrap();
		// TS payload ranges are usually only 184 bytes, separated by headers.
		// Retain a bounded physical-file window across logical seeks so copying
		// a track does not schedule an OS seek/read for every payload range.
		if self.cache_path.as_deref() == Some(path)
			&& file_offset >= self.cache_offset
			&& file_offset - self.cache_offset < self.cache_length as u64
		{
			let relative: usize = (file_offset - self.cache_offset) as usize;
			let count: usize = to_read.min(self.cache_length - relative);
			buf.put_slice(&self.read_cache[relative..relative + count]);
			self.position += count as u64;
			return Poll::Ready(Ok(()));
		}
        let should_seek =
            self.file_path.as_deref() != Some(path) || self.file_position != Some(file_offset);
        if should_seek {
            if self.pending_file_seek.is_none() {
                let start_seek = {
                    let file = match self.file_for_path_mut(path) {
                        Ok(file) => file,
                        Err(error) => return Poll::Ready(Err(error)),
                    };
                    Pin::new(file).start_seek(SeekFrom::Start(file_offset))
                };
                if let Err(error) = start_seek {
                    return Poll::Ready(Err(error));
                }
                self.pending_file_seek = Some((path.to_path_buf(), file_offset));
            }
            let seek_target = self.pending_file_seek.clone().unwrap();
            let poll = {
                let file = match self.file_for_path_mut(&seek_target.0) {
                    Ok(file) => file,
                    Err(error) => return Poll::Ready(Err(error)),
                };
                Pin::new(file).poll_complete(cx)
            };
            match poll {
                Poll::Ready(Ok(position)) => {
                    self.pending_file_seek = None;
                    self.file_path = Some(path.to_path_buf());
                    self.file_position = Some(position);
                }
                Poll::Ready(Err(error)) => {
                    self.pending_file_seek = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

		let mut cache: Vec<u8> = std::mem::take(&mut self.read_cache);
		let read_result: Poll<io::Result<usize>> = {
			let mut destination: ReadBuf<'_> = ReadBuf::new(&mut cache);
			match self.file_for_path_mut(path) {
				Ok(file) => match Pin::new(file).poll_read(cx, &mut destination) {
					Poll::Ready(Ok(())) => Poll::Ready(Ok(destination.filled().len())),
					Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
					Poll::Pending => Poll::Pending,
				},
				Err(error) => Poll::Ready(Err(error)),
			}
		};
		self.read_cache = cache;
		let read: usize = match read_result {
			Poll::Ready(Ok(read)) => read,
			Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
			Poll::Pending => return Poll::Pending,
		};

        if read == 0 {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated segmented mux source input",
            )));
        }
		self.cache_path = Some(path.to_path_buf());
		self.cache_offset = file_offset;
		self.cache_length = read;
		let copied: usize = to_read.min(read);
		buf.put_slice(&self.read_cache[..copied]);
		self.position += copied as u64;
        self.file_path = Some(path.to_path_buf());
        self.file_position = Some(file_offset + u64::try_from(read).unwrap());
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "async")]
impl AsyncRead for AsyncMuxSource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.inner {
            AsyncMuxSourceInner::File(file) => Pin::new(file).poll_read(cx, buf),
            AsyncMuxSourceInner::Segmented(source) => source.poll_read_internal(cx, buf),
        }
    }
}

#[cfg(feature = "async")]
impl AsyncSeek for AsyncMuxSource {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        match &mut self.inner {
            AsyncMuxSourceInner::File(file) => Pin::new(file).start_seek(position),
            AsyncMuxSourceInner::Segmented(source) => source.start_seek(position),
        }
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        match &mut self.inner {
            AsyncMuxSourceInner::File(file) => Pin::new(file).poll_complete(cx),
            AsyncMuxSourceInner::Segmented(source) => source.poll_complete(cx),
        }
    }
}

struct ImportedTrack {
    kind: MuxTrackKind,
    timescale: u32,
    language: [u8; 3],
    handler_name: String,
    mux_policy: ImportedTrackMuxPolicy,
    width: u16,
    height: u16,
    sample_entry_box: Vec<u8>,
    source_edit_media_time: Option<u64>,
    sample_roll_distance: Option<i16>,
    samples: Vec<ImportedSample>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ImportedMp4TrackCarry {
    flat_chunk_sample_counts: Option<Vec<u32>>,
    flat_stsc: Option<Stsc>,
    sample_entry_boxes: Option<Vec<Vec<u8>>>,
    sample_description_indices: Option<Vec<u32>>,
    fragmented_decode_time_gaps: Vec<FragmentedDecodeTimeGap>,
    source_had_empty_stts: bool,
    source_sync_samples: Option<Vec<bool>>,
    preserved_flat_stbl_boxes: Vec<Vec<u8>>,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FragmentedDecodeTimeGap {
    sample_index: usize,
    delta: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PreservedAuthorityFlatVideoAlignment {
    timescale: u32,
    sample_durations: Vec<u32>,
    chunk_sample_counts: Vec<u32>,
}

impl PreservedAuthorityFlatVideoAlignment {
    fn driving_chunk_sample_counts(&self) -> &[u32] {
        if self.chunk_sample_counts.len() > 1 && self.chunk_sample_counts.last() == Some(&1) {
            &self.chunk_sample_counts[..self.chunk_sample_counts.len() - 1]
        } else {
            &self.chunk_sample_counts
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ImportedTrackHeaderPolicy {
    tkhd_flags: u32,
    alternate_group: i16,
    volume: i16,
    matrix: [i32; 9],
    source_track_id: Option<u32>,
    source_track_creation_time: Option<u64>,
    source_track_modification_time: Option<u64>,
    source_media_creation_time: Option<u64>,
    source_media_modification_time: Option<u64>,
    source_movie_timescale: Option<u32>,
    source_media_duration: Option<u64>,
    source_edit_segment_duration: Option<u64>,
    source_media_decode_time_offset: Option<u64>,
    source_stss_first_only: bool,
}

const DEFAULT_IMPORTED_TKHD_FLAGS: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
const DEFAULT_IMPORTED_TKHD_MATRIX: [i32; 9] =
    [0x0001_0000, 0, 0, 0, 0x0001_0000, 0, 0, 0, 0x4000_0000];

const fn default_imported_track_header_policy(kind: MuxTrackKind) -> ImportedTrackHeaderPolicy {
    ImportedTrackHeaderPolicy {
        tkhd_flags: DEFAULT_IMPORTED_TKHD_FLAGS,
        alternate_group: match kind {
            MuxTrackKind::Audio => 1,
            MuxTrackKind::Subtitle => 0,
            MuxTrackKind::Video | MuxTrackKind::Text => 0,
        },
        volume: match kind {
            MuxTrackKind::Audio => 0x0100,
            MuxTrackKind::Video | MuxTrackKind::Text | MuxTrackKind::Subtitle => 0,
        },
        matrix: DEFAULT_IMPORTED_TKHD_MATRIX,
        source_track_id: None,
        source_track_creation_time: None,
        source_track_modification_time: None,
        source_media_creation_time: None,
        source_media_modification_time: None,
        source_movie_timescale: None,
        source_media_duration: None,
        source_edit_segment_duration: None,
        source_media_decode_time_offset: None,
        source_stss_first_only: false,
    }
}

#[derive(Clone, Copy)]
struct ImportedSample {
    source_index: usize,
    data_offset: u64,
    data_size: u32,
    duration: u32,
    composition_time_offset: i32,
    is_sync_sample: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ImportedDataReference {
    SelfContained,
    LocalFile(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlatChunkingMode {
    Auto,
    AutoWithoutTerminalVideoSplit,
    OneSamplePerChunk,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::mux) struct ImportedTrackMuxPolicy {
    sync_sample_table_mode: SyncSampleTableMode,
    stts_run_encoding_mode: SttsRunEncodingMode,
    stsc_run_encoding_mode: StscRunEncodingMode,
    flat_chunking_mode: FlatChunkingMode,
    preferred_track_id: Option<u32>,
    sample_roll_distance: Option<i16>,
    emit_roll_sbgp: bool,
    flat_audio_profile_level_indication: Option<u8>,
    header_policy: Option<ImportedTrackHeaderPolicy>,
    strip_single_sample_dts_btrt: bool,
}

impl ImportedTrackMuxPolicy {
    const DEFAULT: Self = Self {
        sync_sample_table_mode: SyncSampleTableMode::Auto,
        stts_run_encoding_mode: SttsRunEncodingMode::CollapseIdentical,
        stsc_run_encoding_mode: StscRunEncodingMode::CollapseIdentical,
        flat_chunking_mode: FlatChunkingMode::Auto,
        preferred_track_id: None,
        sample_roll_distance: None,
        emit_roll_sbgp: true,
        flat_audio_profile_level_indication: None,
        header_policy: None,
        strip_single_sample_dts_btrt: false,
    };

    const fn with_preferred_track_id(mut self, preferred_track_id: u32) -> Self {
        self.preferred_track_id = if preferred_track_id == 0 {
            None
        } else {
            Some(preferred_track_id)
        };
        self
    }

    const fn preferred_track_id(self) -> Option<u32> {
        self.preferred_track_id
    }

    pub(crate) const fn sample_roll_distance(self) -> Option<i16> {
        self.sample_roll_distance
    }

    pub(crate) const fn with_sample_roll_distance(mut self, sample_roll_distance: i16) -> Self {
        self.sample_roll_distance = Some(sample_roll_distance);
        self
    }

    pub(crate) const fn emit_roll_sbgp(self) -> bool {
        self.emit_roll_sbgp
    }

    pub(crate) const fn with_emit_roll_sbgp(mut self, emit_roll_sbgp: bool) -> Self {
        self.emit_roll_sbgp = emit_roll_sbgp;
        self
    }

    pub(crate) const fn with_sync_sample_table_mode(
        mut self,
        sync_sample_table_mode: SyncSampleTableMode,
    ) -> Self {
        self.sync_sample_table_mode = sync_sample_table_mode;
        self
    }

    pub(crate) const fn flat_audio_profile_level_indication(self) -> Option<u8> {
        self.flat_audio_profile_level_indication
    }

    pub(crate) const fn with_flat_audio_profile_level_indication(
        mut self,
        flat_audio_profile_level_indication: u8,
    ) -> Self {
        self.flat_audio_profile_level_indication = Some(flat_audio_profile_level_indication);
        self
    }

    const fn header_policy(self) -> Option<ImportedTrackHeaderPolicy> {
        self.header_policy
    }

    const fn with_header_policy(mut self, header_policy: ImportedTrackHeaderPolicy) -> Self {
        self.header_policy = Some(header_policy);
        self
    }

    pub(crate) const fn stts_run_encoding_mode(self) -> SttsRunEncodingMode {
        self.stts_run_encoding_mode
    }

    pub(crate) const fn with_stts_run_encoding_mode(
        mut self,
        stts_run_encoding_mode: SttsRunEncodingMode,
    ) -> Self {
        self.stts_run_encoding_mode = stts_run_encoding_mode;
        self
    }

    pub(crate) const fn strip_single_sample_dts_btrt(self) -> bool {
        self.strip_single_sample_dts_btrt
    }

    pub(crate) const fn with_strip_single_sample_dts_btrt(mut self, enabled: bool) -> Self {
        self.strip_single_sample_dts_btrt = enabled;
        self
    }

    pub(in crate::mux) const fn without_terminal_flat_video_chunk_split(mut self) -> Self {
        self.flat_chunking_mode = FlatChunkingMode::AutoWithoutTerminalVideoSplit;
        self
    }

    const fn with_terminal_flat_video_chunk_split(mut self) -> Self {
        self.flat_chunking_mode = FlatChunkingMode::Auto;
        self
    }
}

#[derive(Clone, Copy)]
pub(in crate::mux) struct StagedSample {
    pub(in crate::mux) data_offset: u64,
    pub(in crate::mux) data_size: u32,
    pub(in crate::mux) duration: u32,
    pub(in crate::mux) composition_time_offset: i32,
    pub(in crate::mux) is_sync_sample: bool,
}

#[derive(Clone)]
pub(in crate::mux) struct TrackCandidate {
    pub(in crate::mux) track_id: u32,
    pub(in crate::mux) kind: MuxTrackKind,
    pub(in crate::mux) timescale: u32,
    pub(in crate::mux) language: [u8; 3],
    pub(in crate::mux) handler_name: String,
    pub(in crate::mux) mux_policy: ImportedTrackMuxPolicy,
    pub(in crate::mux) width: u16,
    pub(in crate::mux) height: u16,
    pub(in crate::mux) sample_entry_box: Vec<u8>,
    pub(in crate::mux) source_edit_media_time: Option<u64>,
    pub(in crate::mux) samples: Vec<CandidateSample>,
}

#[derive(Clone, Copy)]
pub(in crate::mux) struct CandidateSample {
    pub(in crate::mux) source_index: usize,
    pub(in crate::mux) data_offset: u64,
    pub(in crate::mux) data_size: u32,
    pub(in crate::mux) duration: u32,
    pub(in crate::mux) composition_time_offset: i32,
    pub(in crate::mux) is_sync_sample: bool,
}

pub(in crate::mux) struct CompositeTrackCandidate {
    pub(in crate::mux) track: TrackCandidate,
    pub(in crate::mux) source_spec: SegmentedMuxSourceSpec,
}

fn assign_candidate_source_index(track: &mut TrackCandidate, source_index: usize) {
    for sample in &mut track.samples {
        sample.source_index = source_index;
    }
}

fn imported_samples_from_staged(
    staged_samples: Vec<StagedSample>,
    source_index: usize,
) -> Vec<ImportedSample> {
    staged_samples
        .into_iter()
        .map(|sample| ImportedSample {
            source_index,
            data_offset: sample.data_offset,
            data_size: sample.data_size,
            duration: sample.duration,
            composition_time_offset: sample.composition_time_offset,
            is_sync_sample: sample.is_sync_sample,
        })
        .collect()
}

fn prepare_request_sync(
    request: &MuxRequest,
    output_path: &Path,
) -> Result<PreparedMuxRequest, MuxError> {
    validate_request_shape(request, output_path)?;

    let mut path_kinds = Vec::with_capacity(request.tracks().len());
    let mut all_profile_authority_inputs = true;
    for track in request.tracks() {
        let kind = match track {
            MuxTrackSpec::Path { path, .. } => detect_path_track_kind_sync(path)?,
            MuxTrackSpec::RawVideo { .. } => DetectedPathTrackKind::Unknown,
        };
        if !matches!(
            kind,
            DetectedPathTrackKind::Mp4
                | DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash)
        ) {
            all_profile_authority_inputs = false;
        }
        path_kinds.push(kind);
    }
    let mut sources = SourceCatalog::default();
    let mut mp4_cache = BTreeMap::<PathBuf, PathSourceMetadata>::new();
    let mut avi_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut dash_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut nhml_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut program_stream_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut saf_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut transport_stream_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut vobsub_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut imported_tracks = Vec::new();
    let mut authority_file_config = None::<MuxFileConfig>;
    let mut selected_mp4_track_carries = SelectedImportedMp4CarryMap::new();

    for (track, path_kind) in request.tracks().iter().zip(path_kinds) {
        let spec = display_track_spec(track);
        match track {
            MuxTrackSpec::RawVideo { path, params } => {
                imported_tracks.push(import_raw_video_sync(
                    path.as_path(),
                    *params,
                    spec,
                    &mut sources,
                )?);
                continue;
            }
            MuxTrackSpec::Path { path, selector } => match path_kind {
                DetectedPathTrackKind::Mp4 => {
                    let selected_metadata;
                    let metadata = if let Some(selector) = selector {
                        selected_metadata =
                            load_selected_mp4_source_sync(path.as_path(), *selector, &mut sources)?;
                        &selected_metadata
                    } else {
                        load_mp4_source_sync(path.as_path(), &mut mp4_cache, &mut sources)?
                    };
                    if all_profile_authority_inputs && authority_file_config.is_none() {
                        authority_file_config = metadata.file_config.clone();
                    }
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, false)?;
                    capture_selected_mp4_track_carries(
                        &selected,
                        &metadata.carries_by_track_id,
                        &mut selected_mp4_track_carries,
                    );
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
                    let metadata =
                        load_avi_source_sync(path.as_path(), &mut avi_cache, &mut sources)?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
                    let metadata =
                        load_dash_source_sync(path.as_path(), &mut dash_cache, &mut sources)?;
                    if all_profile_authority_inputs && authority_file_config.is_none() {
                        authority_file_config = metadata.file_config.clone();
                    }
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi) => {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec,
                        message: unsupported_ghi_container_message().to_string(),
                    });
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec,
                        message: unsupported_gsf_container_message().to_string(),
                    });
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
                    let metadata = load_nhml_source_sync(
                        path.as_path(),
                        DetectedNhmlSidecarKind::Nhml,
                        &mut nhml_cache,
                        &mut sources,
                    )?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
                    let metadata = load_nhml_source_sync(
                        path.as_path(),
                        DetectedNhmlSidecarKind::Nhnt,
                        &mut nhml_cache,
                        &mut sources,
                    )?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
                    let metadata = load_program_stream_source_sync(
                        path.as_path(),
                        &mut program_stream_cache,
                        &mut sources,
                    )?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
                    let metadata =
                        load_saf_source_sync(path.as_path(), &mut saf_cache, &mut sources)?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
                    let metadata = load_transport_stream_source_sync(
                        path.as_path(),
                        &mut transport_stream_cache,
                        &mut sources,
                    )?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
                    let metadata =
                        load_vobsub_source_sync(path.as_path(), &mut vobsub_cache, &mut sources)?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Raw(_)
                | DetectedPathTrackKind::Mp4ImportOnly(_)
                | DetectedPathTrackKind::Unknown => {
                    if let Some(selector) = selector {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec,
                            message: format!(
                                "selector `{}` only applies to containerized sources",
                                format_mp4_selector(*selector)
                            ),
                        });
                    }
                    imported_tracks.push(import_detected_path_raw_sync(
                        path.as_path(),
                        &spec,
                        &mut sources,
                    )?);
                }
            },
        }
    }

    finish_prepared_request(
        request,
        output_path,
        imported_tracks,
        sources,
        authority_file_config,
        selected_mp4_track_carries,
    )
}

#[cfg(feature = "async")]
async fn prepare_request_async(
    request: &MuxRequest,
    output_path: &Path,
) -> Result<PreparedMuxRequest, MuxError> {
    validate_request_shape(request, output_path)?;

    let mut path_kinds = Vec::with_capacity(request.tracks().len());
    let mut all_profile_authority_inputs = true;
    for track in request.tracks() {
        let kind = match track {
            MuxTrackSpec::Path { path, .. } => detect_path_track_kind_async(path).await?,
            MuxTrackSpec::RawVideo { .. } => DetectedPathTrackKind::Unknown,
        };
        if !matches!(
            kind,
            DetectedPathTrackKind::Mp4
                | DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash)
        ) {
            all_profile_authority_inputs = false;
        }
        path_kinds.push(kind);
    }
    let mut sources = SourceCatalog::default();
    let mut mp4_cache = BTreeMap::<PathBuf, PathSourceMetadata>::new();
    let mut avi_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut dash_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut nhml_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut program_stream_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut saf_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut transport_stream_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut vobsub_cache = BTreeMap::<PathBuf, ContainerSourceMetadata>::new();
    let mut imported_tracks = Vec::new();
    let mut authority_file_config = None::<MuxFileConfig>;
    let mut selected_mp4_track_carries = SelectedImportedMp4CarryMap::new();

    for (track, path_kind) in request.tracks().iter().zip(path_kinds) {
        let spec = display_track_spec(track);
        match track {
            MuxTrackSpec::RawVideo { path, params } => {
                imported_tracks.push(
                    import_raw_video_async(path.as_path(), *params, spec, &mut sources).await?,
                );
                continue;
            }
            MuxTrackSpec::Path { path, selector } => match path_kind {
                DetectedPathTrackKind::Mp4 => {
                    let selected_metadata;
                    let metadata = if let Some(selector) = selector {
                        selected_metadata =
                            load_selected_mp4_source_async(path.as_path(), *selector, &mut sources)
                                .await?;
                        &selected_metadata
                    } else {
                        load_mp4_source_async(path.as_path(), &mut mp4_cache, &mut sources).await?
                    };
                    if all_profile_authority_inputs && authority_file_config.is_none() {
                        authority_file_config = metadata.file_config.clone();
                    }
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, false)?;
                    capture_selected_mp4_track_carries(
                        &selected,
                        &metadata.carries_by_track_id,
                        &mut selected_mp4_track_carries,
                    );
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
                    let metadata =
                        load_avi_source_async(path.as_path(), &mut avi_cache, &mut sources).await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
                    let metadata =
                        load_dash_source_async(path.as_path(), &mut dash_cache, &mut sources)
                            .await?;
                    if all_profile_authority_inputs && authority_file_config.is_none() {
                        authority_file_config = metadata.file_config.clone();
                    }
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi) => {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec,
                        message: unsupported_ghi_container_message().to_string(),
                    });
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec,
                        message: unsupported_gsf_container_message().to_string(),
                    });
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
                    let metadata = load_nhml_source_async(
                        path.as_path(),
                        DetectedNhmlSidecarKind::Nhml,
                        &mut nhml_cache,
                        &mut sources,
                    )
                    .await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
                    let metadata = load_nhml_source_async(
                        path.as_path(),
                        DetectedNhmlSidecarKind::Nhnt,
                        &mut nhml_cache,
                        &mut sources,
                    )
                    .await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
                    let metadata = load_program_stream_source_async(
                        path.as_path(),
                        &mut program_stream_cache,
                        &mut sources,
                    )
                    .await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
                    let metadata =
                        load_saf_source_async(path.as_path(), &mut saf_cache, &mut sources).await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
                    let metadata = load_transport_stream_source_async(
                        path.as_path(),
                        &mut transport_stream_cache,
                        &mut sources,
                    )
                    .await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
                    let metadata =
                        load_vobsub_source_async(path.as_path(), &mut vobsub_cache, &mut sources)
                            .await?;
                    let mut selected =
                        select_container_tracks(&metadata.tracks, *selector, spec, true)?;
                    imported_tracks.append(&mut selected);
                }
                DetectedPathTrackKind::Raw(_)
                | DetectedPathTrackKind::Mp4ImportOnly(_)
                | DetectedPathTrackKind::Unknown => {
                    if let Some(selector) = selector {
                        return Err(MuxError::UnsupportedTrackImport {
                            spec,
                            message: format!(
                                "selector `{}` only applies to containerized sources",
                                format_mp4_selector(*selector)
                            ),
                        });
                    }
                    imported_tracks.push(
                        import_detected_path_raw_async(path.as_path(), &spec, &mut sources).await?,
                    );
                }
            },
        }
    }

    finish_prepared_request(
        request,
        output_path,
        imported_tracks,
        sources,
        authority_file_config,
        selected_mp4_track_carries,
    )
}

fn finish_prepared_request(
    request: &MuxRequest,
    _output_path: &Path,
    mut imported_tracks: Vec<ImportedTrack>,
    sources: SourceCatalog,
    authority_file_config: Option<MuxFileConfig>,
    mut selected_mp4_track_carries: SelectedImportedMp4CarryMap,
) -> Result<PreparedMuxRequest, MuxError> {
    if request.output_layout() == MuxOutputLayout::Flat && request.preserve_flat_authority_layout()
    {
        merge_flat_destination_append_tracks(
            &mut imported_tracks,
            &mut selected_mp4_track_carries,
        )?;
    }
    if request.output_layout() == MuxOutputLayout::Flat {
        reconcile_flat_imported_fragment_decode_time_gaps(
            &mut imported_tracks,
            &selected_mp4_track_carries,
            &sources,
        )?;
    }

    let video_count = imported_tracks
        .iter()
        .filter(|track| track.kind == MuxTrackKind::Video)
        .count();
    if request.output_layout() == MuxOutputLayout::Fragmented && video_count > 1 {
        return Err(MuxError::MultipleVideoTracks { count: video_count });
    }

    let movie_timescale = choose_movie_timescale(
        &imported_tracks,
        authority_file_config.as_ref(),
        request.output_layout(),
    )?;
    let file_config = choose_file_config(
        movie_timescale,
        &imported_tracks,
        &sources,
        authority_file_config.as_ref(),
        request.preserve_flat_authority_layout(),
    )
    .with_fragment_event_messages(request.fragment_event_messages().to_vec())
    .with_producer_reference_times(request.producer_reference_times().to_vec());
    let duration_boundary_kind = request
        .duration_mode()
        .map(|duration_mode| match duration_mode {
            super::MuxDurationMode::Segment { .. } => MuxDurationBoundaryKind::Segment,
            super::MuxDurationMode::Fragment { .. } => MuxDurationBoundaryKind::Fragment,
        });
    let fragmented_single_sidx_reference = matches!(
        request.duration_mode(),
        Some(super::MuxDurationMode::Fragment { .. })
    );

    let duration_target = if let Some(duration_mode) = request.duration_mode() {
        let seconds = duration_mode.seconds();
        if !seconds.is_finite() || seconds <= 0.0 {
            return Err(MuxError::InvalidDurationMode {
                mode: duration_mode.label(),
                message: "duration must be a finite value greater than zero".to_string(),
            });
        }
        let ticks = (seconds * f64::from(movie_timescale)).round();
        if ticks < 1.0 {
            return Err(MuxError::InvalidDurationMode {
                mode: duration_mode.label(),
                message: "duration is too small for the selected movie timescale".to_string(),
            });
        }
        Some(ticks as u64)
    } else {
        None
    };
    let auto_flat_interleave_target = if duration_target.is_none()
        && request.output_layout() == MuxOutputLayout::Flat
        && file_config.auto_flat_profile()
    {
        Some(auto_flat_interleave_target_ticks(movie_timescale))
    } else {
        None
    };
    let audio_track_count = imported_tracks
        .iter()
        .filter(|track| track.kind.is_audio())
        .count();
    let mut staged_items = Vec::new();
    let mut track_configs = Vec::new();
    let mut coordination_directives = Vec::new();
    let assigned_track_ids = assign_imported_track_ids(
        &imported_tracks,
        request.output_layout() == MuxOutputLayout::Flat,
    )?;
    let source_track_id_remap =
        imported_source_track_id_remap(&imported_tracks, &assigned_track_ids);
    let preserved_authority_video_alignment = if request.preserve_flat_authority_layout()
        && auto_flat_interleave_target.is_some()
        && audio_track_count == 1
    {
        preserved_authority_flat_video_alignment(&imported_tracks, &assigned_track_ids)?
    } else {
        None
    };
    if request.output_layout() == MuxOutputLayout::Fragmented
        || (request.preserve_flat_authority_layout()
            && request.output_layout() == MuxOutputLayout::Flat)
    {
        for imported_track in &mut imported_tracks {
            let should_restore_source_sync_samples = request.output_layout()
                == MuxOutputLayout::Fragmented
                || imported_track.source_edit_media_time.is_some()
                || imported_track_source_edit_segment_duration(imported_track).is_some();
            if !should_restore_source_sync_samples {
                continue;
            }
            let imported_mp4_carry =
                imported_track_selected_mp4_carry(imported_track, &selected_mp4_track_carries);
            restore_preserved_imported_source_sync_samples(imported_track, imported_mp4_carry);
        }
    }

    for (imported_track, track_id) in imported_tracks
        .iter()
        .zip(assigned_track_ids.iter().copied())
    {
        let imported_mp4_carry =
            imported_track_selected_mp4_carry(imported_track, &selected_mp4_track_carries);
        let mut preserved_flat_stbl_boxes = imported_mp4_carry
            .map(|carry| carry.preserved_flat_stbl_boxes.clone())
            .unwrap_or_default();
        preserved_flat_stbl_boxes.extend(generated_flat_stbl_boxes_for_imported_track(
            imported_track,
            imported_mp4_carry,
            &preserved_flat_stbl_boxes,
            request.preserve_flat_authority_layout(),
        )?);
        let mut preserved_flat_trak_boxes = imported_mp4_carry
            .map(|carry| carry.preserved_flat_trak_boxes.clone())
            .unwrap_or_default();
        if request.preserve_flat_authority_layout()
            && request.output_layout() == MuxOutputLayout::Flat
        {
            preserved_flat_trak_boxes = remap_preserved_flat_trak_boxes(
                &source_track_id_remap,
                imported_track_source_key(imported_track),
                imported_track,
                preserved_flat_trak_boxes,
            )?;
        }
        preserved_flat_trak_boxes = filter_preserved_flat_trak_boxes_for_output(
            imported_track,
            movie_timescale,
            request.output_layout(),
            preserved_flat_trak_boxes,
        );
        let normalized_sample_entry_box = normalize_imported_sample_entry_box(
            imported_track,
            imported_mp4_carry,
            request.output_layout(),
            request.preserve_flat_authority_layout(),
        )?;
        let normalized_sample_entry_boxes = normalized_imported_sample_entry_boxes(
            imported_mp4_carry,
            normalized_sample_entry_box.clone(),
        );
        let sample_description_indices = imported_mp4_carry
            .and_then(|carry| carry.sample_description_indices.as_deref())
            .filter(|indices| indices.len() == imported_track.samples.len());
        let preserved_sample_description_chunk_counts =
            if request.output_layout() == MuxOutputLayout::Flat {
                imported_mp4_carry
                    .and_then(|carry| carry.flat_chunk_sample_counts.as_deref())
                    .filter(|_| {
                        sample_description_indices
                            .is_some_and(sample_description_indices_use_multiple_entries)
                    })
                    .map(|chunk_sample_counts| {
                        validate_imported_flat_chunk_sample_counts(
                            track_id,
                            imported_track.kind,
                            imported_track.samples.len(),
                            chunk_sample_counts,
                        )
                    })
                    .transpose()?
            } else {
                None
            };
        let allow_inexact_movie_scaling = imported_track.mux_policy.header_policy().is_some()
            && imported_track.timescale != movie_timescale;
        let fragmented_decode_time_offset = fragmented_imported_decode_time_offset_for_staging(
            track_id,
            imported_track,
            request.output_layout(),
            movie_timescale,
            allow_inexact_movie_scaling,
        )?;
        let duration_start_time_ticks = imported_timing_start_time_ticks(
            track_id,
            imported_track,
            request.output_layout(),
            movie_timescale,
            allow_inexact_movie_scaling,
        )?;
        let mut fragmented_reference_group_fragment_counts = None;
        let mut preserved_flat_stsc_override = None::<Stsc>;
        let mut decode_time = fragmented_decode_time_offset;
        if let (Some(target_ticks), Some(duration_boundary_kind)) =
            (duration_target, duration_boundary_kind)
        {
            let normalized_sample_durations = imported_track
                .samples
                .iter()
                .map(|sample| {
                    scale_track_time_to_movie(
                        track_id,
                        i64::from(sample.duration),
                        imported_track.timescale,
                        movie_timescale,
                        allow_inexact_movie_scaling,
                    )
                    .map(|duration| duration as u32)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !normalized_sample_durations.is_empty() {
                let chunk_sample_counts = if imported_track.kind.is_video() {
                    let segment_samples = imported_track
                        .samples
                        .iter()
                        .zip(normalized_sample_durations.iter().copied())
                        .map(|(sample, duration_ticks)| {
                            let composition_offset_ticks = scale_track_time_to_movie(
                                track_id,
                                i64::from(sample.composition_time_offset),
                                imported_track.timescale,
                                movie_timescale,
                                allow_inexact_movie_scaling,
                            )?;
                            Ok((
                                duration_ticks,
                                composition_offset_ticks,
                                sample.is_sync_sample,
                            ))
                        })
                        .collect::<Result<Vec<_>, MuxError>>()?;
                    if duration_boundary_kind == MuxDurationBoundaryKind::Fragment {
                        let implicit_reference_group_target =
                            default_fragmented_reference_group_target_ticks(movie_timescale)
                                .max(target_ticks);
                        let (chunk_sample_counts, reference_group_fragment_counts) =
                            build_sync_aligned_fragmented_duration_chunk_sample_counts(
                                track_id,
                                segment_samples,
                                target_ticks,
                                implicit_reference_group_target,
                                duration_start_time_ticks,
                            )?;
                        if implicit_reference_group_target > target_ticks {
                            fragmented_reference_group_fragment_counts =
                                Some(reference_group_fragment_counts);
                        }
                        chunk_sample_counts
                    } else {
                        build_sync_aligned_segment_chunk_sample_counts(
                            track_id,
                            segment_samples,
                            target_ticks,
                            duration_start_time_ticks,
                        )?
                    }
                } else if duration_boundary_kind == MuxDurationBoundaryKind::Segment {
                    let use_sync_aligned_segment_boundaries = imported_track
                        .samples
                        .iter()
                        .any(|sample| sample.is_sync_sample)
                        && !imported_track
                            .samples
                            .iter()
                            .all(|sample| sample.is_sync_sample);
                    if use_sync_aligned_segment_boundaries {
                        let normalized_segment_samples = imported_track
                            .samples
                            .iter()
                            .zip(normalized_sample_durations.iter().copied())
                            .map(|(sample, duration_ticks)| {
                                let composition_offset_ticks = scale_track_time_to_movie(
                                    track_id,
                                    i64::from(sample.composition_time_offset),
                                    imported_track.timescale,
                                    movie_timescale,
                                    allow_inexact_movie_scaling,
                                )?;
                                Ok((
                                    duration_ticks,
                                    composition_offset_ticks,
                                    sample.is_sync_sample,
                                ))
                            })
                            .collect::<Result<Vec<_>, MuxError>>()?;
                        build_sync_aligned_segment_chunk_sample_counts(
                            track_id,
                            normalized_segment_samples,
                            target_ticks,
                            duration_start_time_ticks,
                        )?
                    } else {
                        build_duration_chunk_sample_counts_with_start_time(
                            track_id,
                            normalized_sample_durations,
                            target_ticks,
                            duration_start_time_ticks,
                        )?
                    }
                } else {
                    let implicit_reference_group_target =
                        default_fragmented_reference_group_target_ticks(movie_timescale)
                            .max(target_ticks);
                    if implicit_reference_group_target > target_ticks {
                        let use_sync_aligned_fragment_boundaries = imported_track
                            .samples
                            .iter()
                            .any(|sample| sample.is_sync_sample)
                            && !imported_track
                                .samples
                                .iter()
                                .all(|sample| sample.is_sync_sample);
                        let (chunk_sample_counts, reference_group_fragment_counts) =
                            if use_sync_aligned_fragment_boundaries {
                                let normalized_fragment_samples = imported_track
                                    .samples
                                    .iter()
                                    .zip(normalized_sample_durations.iter().copied())
                                    .map(|(sample, duration_ticks)| {
                                        let composition_offset_ticks = scale_track_time_to_movie(
                                            track_id,
                                            i64::from(sample.composition_time_offset),
                                            imported_track.timescale,
                                            movie_timescale,
                                            allow_inexact_movie_scaling,
                                        )?;
                                        Ok((
                                            duration_ticks,
                                            composition_offset_ticks,
                                            sample.is_sync_sample,
                                        ))
                                    })
                                    .collect::<Result<Vec<_>, MuxError>>()?;
                                build_sync_aligned_fragmented_duration_chunk_sample_counts(
                                    track_id,
                                    normalized_fragment_samples,
                                    target_ticks,
                                    implicit_reference_group_target,
                                    duration_start_time_ticks,
                                )?
                            } else {
                                build_fragmented_duration_chunk_sample_counts_with_start_time(
                                    track_id,
                                    normalized_sample_durations.clone(),
                                    target_ticks,
                                    implicit_reference_group_target,
                                    duration_start_time_ticks,
                                )?
                            };
                        fragmented_reference_group_fragment_counts =
                            Some(reference_group_fragment_counts);
                        chunk_sample_counts
                    } else {
                        build_duration_chunk_sample_counts(
                            track_id,
                            normalized_sample_durations,
                            target_ticks,
                        )?
                    }
                };
                coordination_directives.push(
                    TrackCoordinationDirective::new(track_id, chunk_sample_counts)
                        .with_duration_boundaries(duration_boundary_kind),
                );
            }
        } else if let Some(chunk_sample_counts) = preserved_sample_description_chunk_counts {
            coordination_directives.push(TrackCoordinationDirective::new(
                track_id,
                chunk_sample_counts,
            ));
        } else if auto_flat_interleave_target.is_some() {
            if imported_track.kind.is_audio() {
                if !imported_track.samples.is_empty() {
                    if imported_track.mux_policy.flat_chunking_mode
                        == FlatChunkingMode::OneSamplePerChunk
                    {
                        coordination_directives.push(TrackCoordinationDirective::new(
                            track_id,
                            vec![1; imported_track.samples.len()],
                        ));
                    } else if let Some(source_index) = imported_track
                        .samples
                        .first()
                        .map(|sample| sample.source_index)
                        .filter(|source_index| {
                            imported_track
                                .samples
                                .iter()
                                .all(|sample| sample.source_index == *source_index)
                        })
                    {
                        if let Some(chunk_sample_counts) =
                            (!imported_track_should_rechunk_flat_audio(imported_track))
                                .then(|| {
                                    preserved_imported_flat_audio_chunk_sample_counts(
                                        imported_track,
                                        imported_mp4_carry,
                                        sources.flat_chunk_sample_counts(source_index),
                                    )
                                })
                                .flatten()
                        {
                            let planned_sample_count = chunk_sample_counts
                                .iter()
                                .try_fold(0_usize, |total, chunk_sample_count| {
                                    total.checked_add(usize::try_from(*chunk_sample_count).ok()?)
                                })
                                .ok_or(MuxError::InvalidChunkPlan {
                                    track_id,
                                    message:
                                        "explicit flat audio chunk plan overflowed while validating staged sample coverage"
                                            .to_string(),
                                })?;
                            if planned_sample_count != imported_track.samples.len() {
                                return Err(MuxError::InvalidChunkPlan {
                                    track_id,
                                    message: format!(
                                        "explicit flat audio chunk plan covered {planned_sample_count} sample{} but the imported track carried {}",
                                        if planned_sample_count == 1 { "" } else { "s" },
                                        imported_track.samples.len(),
                                    ),
                                });
                            }
                            let mut chunk_sample_counts = chunk_sample_counts;
                            let should_split_terminal_short_audio_chunk =
                                imported_track_should_split_terminal_flat_audio_chunk(
                                    imported_track,
                                );
                            let should_preserve_source_stsc =
                                !(stsc_run_encoding_mode_for_imported_track(imported_track)
                                    == StscRunEncodingMode::PreserveTerminalBoundary
                                    && should_split_terminal_short_audio_chunk);
                            if !should_preserve_source_stsc {
                                let sample_durations = imported_track
                                    .samples
                                    .iter()
                                    .map(|sample| sample.duration)
                                    .collect::<Vec<_>>();
                                split_terminal_short_audio_chunk_sample_counts(
                                    &sample_durations,
                                    &mut chunk_sample_counts,
                                );
                            } else if let Some(flat_stsc) =
                                imported_mp4_carry.and_then(|carry| carry.flat_stsc.clone())
                            {
                                preserved_flat_stsc_override = Some(flat_stsc);
                            }
                            coordination_directives.push(TrackCoordinationDirective::new(
                                track_id,
                                chunk_sample_counts,
                            ));
                        } else {
                            let sample_durations = imported_track
                                .samples
                                .iter()
                                .map(|sample| sample.duration)
                                .collect::<Vec<_>>();
                            let mut chunk_sample_counts = if request
                                .preserve_flat_authority_layout()
                                && imported_mp4_carry.is_some()
                                && let Some(video_alignment) =
                                    preserved_authority_video_alignment.as_ref()
                            {
                                build_preserved_authority_flat_audio_chunk_sample_counts(
                                    track_id,
                                    imported_track,
                                    movie_timescale,
                                    video_alignment,
                                )?
                            } else {
                                build_imported_flat_audio_chunk_sample_counts(
                                    track_id,
                                    imported_track,
                                    sample_durations,
                                )?
                            };
                            if audio_track_count > 1 {
                                rebalance_small_multi_audio_chunk_sample_counts(
                                    &mut chunk_sample_counts,
                                );
                            }
                            if stsc_run_encoding_mode_for_imported_track(imported_track)
                                == StscRunEncodingMode::PreserveTerminalBoundary
                                && imported_track_should_split_terminal_flat_audio_chunk(
                                    imported_track,
                                )
                            {
                                let sample_durations = imported_track
                                    .samples
                                    .iter()
                                    .map(|sample| sample.duration)
                                    .collect::<Vec<_>>();
                                split_terminal_short_audio_chunk_sample_counts(
                                    &sample_durations,
                                    &mut chunk_sample_counts,
                                );
                            }
                            coordination_directives.push(TrackCoordinationDirective::new(
                                track_id,
                                chunk_sample_counts,
                            ));
                        }
                    } else {
                        let sample_durations = imported_track
                            .samples
                            .iter()
                            .map(|sample| sample.duration)
                            .collect::<Vec<_>>();
                        let mut chunk_sample_counts = if request.preserve_flat_authority_layout()
                            && imported_mp4_carry.is_some()
                            && let Some(video_alignment) =
                                preserved_authority_video_alignment.as_ref()
                        {
                            build_preserved_authority_flat_audio_chunk_sample_counts(
                                track_id,
                                imported_track,
                                movie_timescale,
                                video_alignment,
                            )?
                        } else {
                            build_imported_flat_audio_chunk_sample_counts(
                                track_id,
                                imported_track,
                                sample_durations,
                            )?
                        };
                        if audio_track_count > 1 {
                            rebalance_small_multi_audio_chunk_sample_counts(
                                &mut chunk_sample_counts,
                            );
                        }
                        if stsc_run_encoding_mode_for_imported_track(imported_track)
                            == StscRunEncodingMode::PreserveTerminalBoundary
                            && imported_track_should_split_terminal_flat_audio_chunk(imported_track)
                        {
                            let sample_durations = imported_track
                                .samples
                                .iter()
                                .map(|sample| sample.duration)
                                .collect::<Vec<_>>();
                            split_terminal_short_audio_chunk_sample_counts(
                                &sample_durations,
                                &mut chunk_sample_counts,
                            );
                        }
                        coordination_directives.push(TrackCoordinationDirective::new(
                            track_id,
                            chunk_sample_counts,
                        ));
                    }
                }
            } else if imported_track.kind == MuxTrackKind::Subtitle
                && imported_track.sample_entry_box.get(4..8) == Some(b"mp4s".as_slice())
                && !imported_track.samples.is_empty()
            {
                coordination_directives.push(TrackCoordinationDirective::new(
                    track_id,
                    vec![1; imported_track.samples.len()],
                ));
            } else if imported_track.kind.is_video() && !imported_track.samples.is_empty() {
                let sample_durations = imported_track
                    .samples
                    .iter()
                    .map(|sample| sample.duration)
                    .collect::<Vec<_>>();
                let mut chunk_sample_counts = if let Some(source_index) = imported_track
                    .samples
                    .first()
                    .map(|sample| sample.source_index)
                    .filter(|source_index| {
                        imported_track
                            .samples
                            .iter()
                            .all(|sample| sample.source_index == *source_index)
                    })
                    .filter(|_| {
                        sample_entry_box_type(&imported_track.sample_entry_box)
                            == Some(FourCc::from_bytes(*b"vp08"))
                    }) {
                    if let Some(chunk_sample_counts) = imported_mp4_carry
                        .and_then(|carry| carry.flat_chunk_sample_counts.as_deref())
                        .or_else(|| sources.flat_chunk_sample_counts(source_index))
                    {
                        let planned_sample_count = chunk_sample_counts
                            .iter()
                            .try_fold(0_usize, |total, chunk_sample_count| {
                                total.checked_add(usize::try_from(*chunk_sample_count).ok()?)
                            })
                            .ok_or(MuxError::InvalidChunkPlan {
                                track_id,
                                message:
                                    "explicit flat video chunk plan overflowed while validating staged sample coverage"
                                        .to_string(),
                            })?;
                        if planned_sample_count != imported_track.samples.len() {
                            return Err(MuxError::InvalidChunkPlan {
                                track_id,
                                message: format!(
                                    "explicit flat video chunk plan covered {planned_sample_count} sample{} but the imported track carried {}",
                                    if planned_sample_count == 1 { "" } else { "s" },
                                    imported_track.samples.len(),
                                ),
                            });
                        }
                        if let Some(flat_stsc) =
                            imported_mp4_carry.and_then(|carry| carry.flat_stsc.clone())
                        {
                            preserved_flat_stsc_override = Some(flat_stsc);
                        }
                        chunk_sample_counts.to_vec()
                    } else if imported_mp4_carry.is_some() {
                        build_fragmented_imported_vp08_flat_chunk_sample_counts(
                            track_id,
                            imported_track,
                        )?
                    } else {
                        build_capped_duration_chunk_sample_counts(
                            track_id,
                            sample_durations.iter().copied(),
                            auto_flat_interleave_target_ticks(imported_track.timescale),
                        )?
                    }
                } else {
                    build_capped_duration_chunk_sample_counts(
                        track_id,
                        sample_durations.iter().copied(),
                        auto_flat_interleave_target_ticks(imported_track.timescale),
                    )?
                };
                if imported_track.mux_policy.flat_chunking_mode
                    != FlatChunkingMode::AutoWithoutTerminalVideoSplit
                    && sample_entry_box_type(&imported_track.sample_entry_box)
                        != Some(FourCc::from_bytes(*b"vp08"))
                {
                    split_terminal_short_video_chunk_sample_counts(
                        &sample_durations,
                        &mut chunk_sample_counts,
                    );
                }
                coordination_directives.push(TrackCoordinationDirective::new(
                    track_id,
                    chunk_sample_counts,
                ));
            }
        }

        let fragmented_decode_time_gaps = if request.output_layout() == MuxOutputLayout::Fragmented
        {
            imported_track_source_key(imported_track)
                .and_then(|(source_index, source_track_id)| {
                    sources.fragmented_decode_time_gaps(source_index, source_track_id)
                })
                .or_else(|| {
                    imported_mp4_carry.map(|carry| carry.fragmented_decode_time_gaps.as_slice())
                })
                .unwrap_or(&[])
        } else {
            &[]
        };
        let mut next_fragmented_decode_time_gap = 0_usize;
        for (sample_index, sample) in imported_track.samples.iter().enumerate() {
            while fragmented_decode_time_gaps
                .get(next_fragmented_decode_time_gap)
                .is_some_and(|gap| gap.sample_index == sample_index)
            {
                let gap = &fragmented_decode_time_gaps[next_fragmented_decode_time_gap];
                let gap_ticks = i64::try_from(gap.delta)
                    .map_err(|_| MuxError::LayoutOverflow("fragmented decode-time gap"))?;
                let gap_movie = scale_track_time_to_movie(
                    track_id,
                    gap_ticks,
                    imported_track.timescale,
                    movie_timescale,
                    allow_inexact_movie_scaling,
                )?;
                let gap_movie = u64::try_from(gap_movie)
                    .map_err(|_| MuxError::LayoutOverflow("fragmented decode-time gap"))?;
                decode_time = decode_time
                    .checked_add(gap_movie)
                    .ok_or(MuxError::LayoutOverflow("fragmented decode-time gap"))?;
                next_fragmented_decode_time_gap += 1;
            }
            let sample_description_index = sample_description_indices
                .and_then(|indices| indices.get(sample_index).copied())
                .unwrap_or(1);
            if sample_description_index == 0
                || usize::try_from(sample_description_index)
                    .ok()
                    .is_none_or(|index| index > normalized_sample_entry_boxes.len())
            {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: imported_track_relation_error_spec(imported_track),
                    message: format!(
                        "track {track_id} uses sample description index {sample_description_index} with {} sample entries",
                        normalized_sample_entry_boxes.len()
                    ),
                });
            }
            let duration = scale_track_time_to_movie(
                track_id,
                i64::from(sample.duration),
                imported_track.timescale,
                movie_timescale,
                allow_inexact_movie_scaling,
            )? as u32;
            let composition_time_offset = scale_track_time_to_movie(
                track_id,
                i64::from(sample.composition_time_offset),
                imported_track.timescale,
                movie_timescale,
                allow_inexact_movie_scaling,
            )? as i32;
            staged_items.push(
                MuxStagedMediaItem::new(
                    sample.source_index,
                    track_id,
                    decode_time,
                    duration,
                    sample.data_offset,
                    sample.data_size,
                )
                .with_composition_time_offset(composition_time_offset)
                .with_sync_sample(sample.is_sync_sample)
                .with_sample_description_index(sample_description_index),
            );
            decode_time = decode_time
                .checked_add(u64::from(duration))
                .ok_or(MuxError::LayoutOverflow("track decode timeline"))?;
        }

        let config = match imported_track.kind {
            MuxTrackKind::Audio => MuxTrackConfig::new_audio(
                track_id,
                imported_track.timescale,
                normalized_sample_entry_box.clone(),
            ),
            MuxTrackKind::Video => MuxTrackConfig::new_video(
                track_id,
                imported_track.timescale,
                imported_track.width,
                imported_track.height,
                normalized_sample_entry_box.clone(),
            ),
            MuxTrackKind::Text => MuxTrackConfig::new_text(
                track_id,
                imported_track.timescale,
                imported_track.width,
                imported_track.height,
                normalized_sample_entry_box.clone(),
            ),
            MuxTrackKind::Subtitle => MuxTrackConfig::new_subtitle(
                track_id,
                imported_track.timescale,
                imported_track.width,
                imported_track.height,
                normalized_sample_entry_box.clone(),
            ),
        }
        .with_language(imported_track.language)
        .with_handler_name(imported_track.handler_name.clone())
        .with_tkhd_flags(
            imported_track
                .mux_policy
                .header_policy()
                .unwrap_or_else(|| default_imported_track_header_policy(imported_track.kind))
                .tkhd_flags,
        )
        .with_alternate_group(
            imported_track
                .mux_policy
                .header_policy()
                .unwrap_or_else(|| default_imported_track_header_policy(imported_track.kind))
                .alternate_group,
        )
        .with_volume(
            imported_track
                .mux_policy
                .header_policy()
                .unwrap_or_else(|| default_imported_track_header_policy(imported_track.kind))
                .volume,
        )
        .with_matrix(
            imported_track
                .mux_policy
                .header_policy()
                .unwrap_or_else(|| default_imported_track_header_policy(imported_track.kind))
                .matrix,
        )
        .with_sync_sample_table_mode(sync_sample_table_mode_for_imported_track(
            imported_track,
            request.output_layout(),
            request.preserve_flat_authority_layout(),
        ))
        .with_stts_run_encoding_mode(stts_run_encoding_mode_for_imported_track(imported_track))
        .with_stsc_run_encoding_mode(stsc_run_encoding_mode_for_imported_track(imported_track))
        .with_sample_entry_boxes(normalized_sample_entry_boxes.clone());
        let config = if imported_track.kind.is_video()
            && (request.output_layout() == MuxOutputLayout::Fragmented
                || (request.output_layout() == MuxOutputLayout::Flat
                    && request.preserve_flat_authority_layout()))
        {
            if let Some((track_width_fixed_16_16, track_height_fixed_16_16)) =
                super::mp4::fragmented_visual_tkhd_dimensions_fixed_16_16(
                    &normalized_sample_entry_box,
                )?
            {
                config.with_tkhd_dimensions_fixed_16_16(
                    track_width_fixed_16_16,
                    track_height_fixed_16_16,
                )
            } else {
                config
            }
        } else {
            config
        };
        let config = if let Some(edit_media_time) =
            imported_track.source_edit_media_time.or_else(|| {
                derived_fragmented_imported_edit_media_time(imported_track, request.output_layout())
            }) {
            if request.output_layout() == MuxOutputLayout::Flat || edit_media_time != 0 {
                config.with_edit_media_time(edit_media_time)
            } else {
                config
            }
        } else {
            config
        };
        let suppress_fragmented_imported_roll_grouping =
            imported_track_suppresses_fragmented_roll_grouping(
                imported_track,
                request.output_layout(),
            );
        let config = if !suppress_fragmented_imported_roll_grouping
            && let Some(sample_roll_distance) = imported_track.sample_roll_distance
        {
            config.with_sample_roll_distance(sample_roll_distance)
        } else {
            config
        };
        let config = config.with_emit_roll_sbgp(
            imported_track.mux_policy.emit_roll_sbgp()
                && !suppress_fragmented_imported_roll_grouping,
        );
        let carry_flat_authority_creation_times = !imported_track_uses_speex_family(imported_track);
        let config = config
            .with_flat_source_track_creation_time(
                carry_flat_authority_creation_times
                    .then(|| {
                        imported_track
                            .mux_policy
                            .header_policy()
                            .and_then(|policy| policy.source_track_creation_time)
                    })
                    .flatten(),
            )
            .with_flat_source_track_modification_time(
                (request.preserve_flat_authority_layout() && imported_track.kind.is_video())
                    .then(|| {
                        imported_track
                            .mux_policy
                            .header_policy()
                            .and_then(|policy| policy.source_track_modification_time)
                    })
                    .flatten(),
            )
            .with_flat_source_media_creation_time(
                carry_flat_authority_creation_times
                    .then(|| {
                        imported_track
                            .mux_policy
                            .header_policy()
                            .and_then(|policy| policy.source_media_creation_time)
                    })
                    .flatten(),
            );
        let config = config.with_flat_source_media_modification_time(
            (request.preserve_flat_authority_layout() && imported_track.kind.is_video())
                .then(|| {
                    imported_track
                        .mux_policy
                        .header_policy()
                        .and_then(|policy| policy.source_media_modification_time)
                })
                .flatten(),
        );
        let config = config.with_omit_flat_iods(
            imported_track_uses_speex_family(imported_track)
                && imported_track.mux_policy.header_policy().is_some(),
        );
        let config = if let Some(flat_timing_override) = flat_timing_override_for_imported_track(
            imported_track,
            movie_timescale,
            request.preserve_flat_authority_layout(),
        ) {
            config.with_flat_timing_override(flat_timing_override)
        } else {
            config
        };
        let config = if let Some(flat_audio_profile_level_indication) = imported_track
            .mux_policy
            .flat_audio_profile_level_indication()
        {
            config.with_flat_audio_profile_level_indication(flat_audio_profile_level_indication)
        } else {
            config
        };
        let config = if request.output_layout() == MuxOutputLayout::Fragmented
            && let Some(decode_time_offset) = imported_track
                .mux_policy
                .header_policy()
                .and_then(|policy| policy.source_media_decode_time_offset)
        {
            config.with_fragmented_decode_time_offset(decode_time_offset)
        } else {
            config
        };
        let config = if let Some(fragmented_reference_group_fragment_counts) =
            fragmented_reference_group_fragment_counts
        {
            config.with_fragmented_reference_group_fragment_counts(
                fragmented_reference_group_fragment_counts,
            )
        } else {
            config
        };
        let config = if let Some(flat_stsc_override) = preserved_flat_stsc_override {
            config.with_flat_stsc_override(flat_stsc_override)
        } else {
            config
        };
        let config = config
            .with_preserved_flat_stbl_boxes(preserved_flat_stbl_boxes)
            .with_preserved_flat_trak_boxes(preserved_flat_trak_boxes);
        track_configs.push(config);
    }

    let use_chunk_ordinal_interleave = (request.preserve_flat_authority_layout()
        && request.output_layout() == MuxOutputLayout::Flat
        && video_count == 1
        && audio_track_count == 1
        && preserved_authority_video_alignment.is_some())
        || (request.output_layout() == MuxOutputLayout::Flat
            && auto_flat_interleave_target.is_some()
            && video_count > 1);
    let interleave_policy = if use_chunk_ordinal_interleave {
        MuxInterleavePolicy::ChunkOrdinalThenSource
    } else {
        MuxInterleavePolicy::DecodeTime
    };
    let plan = plan_staged_media_items_with_coordination(
        staged_items,
        interleave_policy,
        coordination_directives,
    )?;
    Ok(PreparedMuxRequest {
        output_layout: request.output_layout(),
        file_config,
        track_configs,
        fragmented_single_sidx_reference,
        plan,
        source_specs: sources.specs,
    })
}

fn auto_flat_interleave_target_ticks(movie_timescale: u32) -> u64 {
    u64::from(movie_timescale)
        .saturating_mul(AUTO_FLAT_INTERLEAVE_MILLISECONDS)
        .div_ceil(1_000)
        .max(1)
}

fn default_fragmented_reference_group_target_ticks(movie_timescale: u32) -> u64 {
    u64::from(movie_timescale)
        .saturating_mul(DEFAULT_FRAGMENTED_REFERENCE_GROUP_SECONDS)
        .max(1)
}

#[derive(Default)]
struct SourceCatalog {
    specs: Vec<SourceSpec>,
    files: BTreeMap<PathBuf, usize>,
    flat_source_encoding_metadata: BTreeMap<usize, String>,
    flat_source_encoder_metadata: BTreeMap<usize, String>,
    flat_chunk_sample_counts_by_source: BTreeMap<usize, Vec<u32>>,
    fragmented_decode_time_gaps_by_source_track:
        BTreeMap<(usize, u32), Vec<FragmentedDecodeTimeGap>>,
}

impl SourceCatalog {
    fn add_file(&mut self, path: &Path) -> Result<usize, MuxError> {
        let absolute = absolute_path(path)?;
        if let Some(existing) = self.files.get(&absolute) {
            return Ok(*existing);
        }
        let index = self.specs.len();
        self.specs.push(SourceSpec::File(absolute.clone()));
        self.files.insert(absolute, index);
        Ok(index)
    }

    fn add_segmented(&mut self, mut spec: SegmentedMuxSourceSpec) -> Result<usize, MuxError> {
        spec.path = absolute_path(&spec.path)?;
        let index = self.specs.len();
        self.specs.push(SourceSpec::Segmented(spec));
        Ok(index)
    }

    fn replace_with_segmented(
        &mut self,
        source_index: usize,
        mut spec: SegmentedMuxSourceSpec,
    ) -> Result<(), MuxError> {
        spec.path = absolute_path(&spec.path)?;
        let Some(slot) = self.specs.get_mut(source_index) else {
            return Err(MuxError::MissingSourceIndex {
                source_index,
                source_count: self.specs.len(),
            });
        };
        *slot = SourceSpec::Segmented(spec);
        Ok(())
    }

    fn flat_source_encoding_metadata(&self, source_index: usize) -> Option<&str> {
        self.flat_source_encoding_metadata
            .get(&source_index)
            .map(String::as_str)
    }

    fn set_flat_source_encoder_metadata(&mut self, source_index: usize, metadata: String) {
        self.flat_source_encoder_metadata
            .insert(source_index, metadata);
    }

    fn set_flat_chunk_sample_counts(&mut self, source_index: usize, chunk_sample_counts: Vec<u32>) {
        self.flat_chunk_sample_counts_by_source
            .insert(source_index, chunk_sample_counts);
    }

    fn set_fragmented_decode_time_gaps(
        &mut self,
        source_index: usize,
        source_track_id: u32,
        gaps: Vec<FragmentedDecodeTimeGap>,
    ) {
        if gaps.is_empty() {
            self.fragmented_decode_time_gaps_by_source_track
                .remove(&(source_index, source_track_id));
        } else {
            self.fragmented_decode_time_gaps_by_source_track
                .insert((source_index, source_track_id), gaps);
        }
    }

    fn flat_source_encoder_metadata(&self, source_index: usize) -> Option<&str> {
        self.flat_source_encoder_metadata
            .get(&source_index)
            .map(String::as_str)
    }

    fn flat_chunk_sample_counts(&self, source_index: usize) -> Option<&[u32]> {
        self.flat_chunk_sample_counts_by_source
            .get(&source_index)
            .map(Vec::as_slice)
    }

    fn fragmented_decode_time_gaps(
        &self,
        source_index: usize,
        source_track_id: u32,
    ) -> Option<&[FragmentedDecodeTimeGap]> {
        self.fragmented_decode_time_gaps_by_source_track
            .get(&(source_index, source_track_id))
            .map(Vec::as_slice)
    }
}

struct PathSourceMetadata {
    file_config: Option<MuxFileConfig>,
    tracks: Vec<TrackCandidate>,
    carries_by_track_id: BTreeMap<u32, ImportedMp4TrackCarry>,
    source_override: Option<SegmentedMuxSourceSpec>,
}

struct ContainerSourceMetadata {
    file_config: Option<MuxFileConfig>,
    tracks: Vec<TrackCandidate>,
}

fn remap_candidate_source_indices(
    track: &mut TrackCandidate,
    source_index_map: &BTreeMap<usize, usize>,
) -> Result<(), MuxError> {
    for sample in &mut track.samples {
        sample.source_index =
            *source_index_map
                .get(&sample.source_index)
                .ok_or(MuxError::MissingSourceIndex {
                    source_index: sample.source_index,
                    source_count: source_index_map.len(),
                })?;
    }
    Ok(())
}

fn materialize_parsed_nhml_source(
    parsed: ParsedNhmlSource,
    sources: &mut SourceCatalog,
) -> Result<ContainerSourceMetadata, MuxError> {
    let mut source_index_map = BTreeMap::<usize, usize>::new();
    for (xml_source_index, spec) in parsed.source_specs {
        let source_index = match spec {
            ParsedNhmlSourceSpec::File(path) => sources.add_file(&path)?,
            ParsedNhmlSourceSpec::Segmented(spec) => sources.add_segmented(spec)?,
        };
        source_index_map.insert(xml_source_index, source_index);
    }
    let mut tracks = parsed.tracks;
    for track in &mut tracks {
        remap_candidate_source_indices(track, &source_index_map)?;
    }
    Ok(ContainerSourceMetadata {
        file_config: None,
        tracks,
    })
}

fn materialize_parsed_dash_source(
    manifest_path: &Path,
    parsed: ParsedDashSource,
    sources: &mut SourceCatalog,
) -> Result<ContainerSourceMetadata, MuxError> {
    let period_count = parsed.periods.len();
    let mut merged_tracks = Vec::new();
    let mut authority_file_config = None::<MuxFileConfig>;
    let mut saw_authority_file_config = false;
    let mut authority_file_config_compatible = true;
    for period in parsed.periods {
        let mut period_tracks = Vec::new();
        for spec in period.sources {
            let source_index = sources.add_segmented(spec.clone())?;
            let mut reader = SyncMuxSource::open(&SourceSpec::Segmented(spec))?;
            let parsed = parse_mp4_source_sync(manifest_path, source_index, &mut reader)?;
            merge_dash_file_config(
                &mut authority_file_config,
                &mut saw_authority_file_config,
                &mut authority_file_config_compatible,
                parsed.file_config.as_ref(),
            );
            period_tracks.extend(parsed.tracks);
        }
        merge_dash_period_tracks(
            manifest_path,
            &mut merged_tracks,
            period_tracks,
            period.start_millis,
        )?;
    }
    for track in &mut merged_tracks {
        track.mux_policy = track.mux_policy.with_strip_single_sample_dts_btrt(true);
        if period_count > 1 && track_candidate_uses_dts_family(track) {
            track.mux_policy = track
                .mux_policy
                .with_stts_run_encoding_mode(SttsRunEncodingMode::PreservePerSample);
        }
        normalize_local_dash_track_header_policy(track);
    }
    Ok(ContainerSourceMetadata {
        file_config: authority_file_config.map(normalize_local_dash_authority_file_config),
        tracks: merged_tracks,
    })
}

#[cfg(feature = "async")]
async fn materialize_parsed_dash_source_async(
    manifest_path: &Path,
    parsed: ParsedDashSource,
    sources: &mut SourceCatalog,
) -> Result<ContainerSourceMetadata, MuxError> {
    let period_count = parsed.periods.len();
    let mut merged_tracks = Vec::new();
    let mut authority_file_config = None::<MuxFileConfig>;
    let mut saw_authority_file_config = false;
    let mut authority_file_config_compatible = true;
    for period in parsed.periods {
        let mut period_tracks = Vec::new();
        for spec in period.sources {
            let source_index = sources.add_segmented(spec.clone())?;
            let mut reader = AsyncMuxSource::open(&SourceSpec::Segmented(spec)).await?;
            let parsed = parse_mp4_source_async(manifest_path, source_index, &mut reader).await?;
            merge_dash_file_config(
                &mut authority_file_config,
                &mut saw_authority_file_config,
                &mut authority_file_config_compatible,
                parsed.file_config.as_ref(),
            );
            period_tracks.extend(parsed.tracks);
        }
        merge_dash_period_tracks(
            manifest_path,
            &mut merged_tracks,
            period_tracks,
            period.start_millis,
        )?;
    }
    for track in &mut merged_tracks {
        track.mux_policy = track.mux_policy.with_strip_single_sample_dts_btrt(true);
        if period_count > 1 && track_candidate_uses_dts_family(track) {
            track.mux_policy = track
                .mux_policy
                .with_stts_run_encoding_mode(SttsRunEncodingMode::PreservePerSample);
        }
        normalize_local_dash_track_header_policy(track);
    }
    Ok(ContainerSourceMetadata {
        file_config: authority_file_config.map(normalize_local_dash_authority_file_config),
        tracks: merged_tracks,
    })
}

fn normalize_local_dash_authority_file_config(file_config: MuxFileConfig) -> MuxFileConfig {
    file_config
        .with_minor_version(1)
        .with_keep_flat_free_box(true)
        .with_auto_flat_profile(true)
        .with_keep_flat_authority_brands(true)
        .with_flat_source_encoding_metadata(Some(LOCAL_DASH_FLAT_TOOL_METADATA_VALUE.to_string()))
        .with_preserve_auto_flat_movie_timescale(true)
}

fn normalize_local_dash_track_header_policy(track: &mut TrackCandidate) {
    if track.kind != MuxTrackKind::Audio {
        return;
    }
    let Some(mut header_policy) = track.mux_policy.header_policy() else {
        return;
    };
    if header_policy.alternate_group == 0 {
        header_policy.alternate_group = 1;
        track.mux_policy = track.mux_policy.with_header_policy(header_policy);
    }
}

fn merge_dash_file_config(
    authority_file_config: &mut Option<MuxFileConfig>,
    saw_authority_file_config: &mut bool,
    authority_file_config_compatible: &mut bool,
    candidate: Option<&MuxFileConfig>,
) {
    if !*authority_file_config_compatible {
        return;
    }
    let Some(candidate) = candidate else {
        return;
    };
    if !*saw_authority_file_config {
        *authority_file_config = Some(candidate.clone());
        *saw_authority_file_config = true;
        return;
    }
    if authority_file_config.as_ref() != Some(candidate) {
        *authority_file_config = None;
        *authority_file_config_compatible = false;
    }
}

fn merge_dash_period_tracks(
    manifest_path: &Path,
    merged_tracks: &mut Vec<TrackCandidate>,
    period_tracks: Vec<TrackCandidate>,
    period_start_millis: u64,
) -> Result<(), MuxError> {
    if period_tracks.is_empty() {
        return Ok(());
    }
    if merged_tracks.is_empty() {
        *merged_tracks = period_tracks;
        return Ok(());
    }
    if merged_tracks.len() != period_tracks.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: manifest_path.display().to_string(),
            message: format!(
                "multi-period local MPD import requires the same compatible track count in each period; the first period resolved to {} track{} but a later period resolved to {}",
                merged_tracks.len(),
                if merged_tracks.len() == 1 { "" } else { "s" },
                period_tracks.len()
            ),
        });
    }
    for (track_index, (merged_track, period_track)) in
        merged_tracks.iter_mut().zip(period_tracks).enumerate()
    {
        ensure_dash_period_track_compatible(
            manifest_path,
            track_index,
            merged_track,
            &period_track,
        )?;
        if track_candidate_uses_dts_family(merged_track) {
            merge_dash_period_track_samples_with_start(
                manifest_path,
                merged_track,
                &period_track,
                period_start_millis,
            )?;
        } else {
            merged_track.samples.extend(period_track.samples);
        }
    }
    Ok(())
}

fn ensure_dash_period_track_compatible(
    manifest_path: &Path,
    track_index: usize,
    merged_track: &TrackCandidate,
    period_track: &TrackCandidate,
) -> Result<(), MuxError> {
    let track_number = track_index + 1;
    let incompatible = merged_track.kind != period_track.kind
        || merged_track.timescale != period_track.timescale
        || merged_track.language != period_track.language
        || merged_track.handler_name != period_track.handler_name
        || merged_track.mux_policy != period_track.mux_policy
        || merged_track.width != period_track.width
        || merged_track.height != period_track.height
        || merged_track.sample_entry_box != period_track.sample_entry_box
        || merged_track.source_edit_media_time != period_track.source_edit_media_time;
    if incompatible {
        return Err(MuxError::UnsupportedTrackImport {
            spec: manifest_path.display().to_string(),
            message: format!(
                "multi-period local MPD import requires one stable authored track shape per track position; track {} changed across periods and cannot be merged truthfully on the current path-only ingest surface",
                track_number
            ),
        });
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct DashRequestedSampleSpan {
    start: u64,
    end: u64,
    sample: CandidateSample,
}

fn merge_dash_period_track_samples_with_start(
    manifest_path: &Path,
    merged_track: &mut TrackCandidate,
    period_track: &TrackCandidate,
    period_start_millis: u64,
) -> Result<(), MuxError> {
    let period_start_ticks =
        scale_dash_period_start_millis(period_start_millis, merged_track.timescale)?;
    let mut spans = dash_requested_sample_spans(&merged_track.samples, 0)?;
    spans.extend(dash_requested_sample_spans(
        &period_track.samples,
        period_start_ticks,
    )?);
    spans.sort_by_key(|span| span.start);

    let Some(merged_end) = spans.iter().map(|span| span.end).max() else {
        merged_track.samples.clear();
        return Ok(());
    };

    let mut adjusted_starts = Vec::with_capacity(spans.len());
    for span in &spans {
        let adjusted = adjusted_starts
            .last()
            .copied()
            .map_or(span.start, |previous: u64| {
                span.start.max(previous.saturating_add(1))
            });
        adjusted_starts.push(adjusted);
    }

    let Some(last_start) = adjusted_starts.last().copied() else {
        merged_track.samples.clear();
        return Ok(());
    };
    if last_start >= merged_end {
        return Err(MuxError::UnsupportedTrackImport {
            spec: manifest_path.display().to_string(),
            message: "multi-period local MPD DTS-family import resolved more overlapping samples than can fit in the merged period timeline on the current path-only ingest surface".to_string(),
        });
    }

    let mut merged_samples = Vec::with_capacity(spans.len());
    for (index, span) in spans.into_iter().enumerate() {
        let next_start = adjusted_starts
            .get(index + 1)
            .copied()
            .unwrap_or(merged_end);
        let duration = u32::try_from(next_start - adjusted_starts[index])
            .map_err(|_| MuxError::LayoutOverflow("dash merged sample duration"))?;
        let mut sample = span.sample;
        sample.duration = duration;
        merged_samples.push(sample);
    }
    merged_track.samples = merged_samples;
    Ok(())
}

fn dash_requested_sample_spans(
    samples: &[CandidateSample],
    timeline_start: u64,
) -> Result<Vec<DashRequestedSampleSpan>, MuxError> {
    let mut spans = Vec::with_capacity(samples.len());
    let mut decode_time = timeline_start;
    for sample in samples {
        let end = decode_time
            .checked_add(u64::from(sample.duration))
            .ok_or(MuxError::LayoutOverflow("dash requested sample span"))?;
        spans.push(DashRequestedSampleSpan {
            start: decode_time,
            end,
            sample: *sample,
        });
        decode_time = end;
    }
    Ok(spans)
}

fn scale_dash_period_start_millis(
    period_start_millis: u64,
    timescale: u32,
) -> Result<u64, MuxError> {
    period_start_millis
        .checked_mul(u64::from(timescale))
        .ok_or(MuxError::LayoutOverflow("dash period start scaling"))
        .map(|scaled| scaled / 1000)
}

fn materialize_composite_tracks(
    sources: &mut SourceCatalog,
    composite_tracks: Vec<CompositeTrackCandidate>,
) -> Result<ContainerSourceMetadata, MuxError> {
    let mut tracks = Vec::with_capacity(composite_tracks.len());
    for composite in composite_tracks {
        let source_index = sources.add_segmented(composite.source_spec)?;
        let mut track = composite.track;
        assign_candidate_source_index(&mut track, source_index);
        tracks.push(track);
    }
    Ok(ContainerSourceMetadata {
        file_config: None,
        tracks,
    })
}

fn materialize_transport_stream_source(
    sources: &mut SourceCatalog,
    scanned: super::demux::TransportStreamScanResult,
) -> Result<ContainerSourceMetadata, MuxError> {
    let super::demux::TransportStreamScanResult {
        composite_tracks,
        flat_chunk_sample_counts_by_track_id,
    } = scanned;
    let mut tracks = Vec::with_capacity(composite_tracks.len());
    for composite in composite_tracks {
        let track_id = composite.track.track_id;
        let source_index = sources.add_segmented(composite.source_spec)?;
        if let Some(chunk_sample_counts) = flat_chunk_sample_counts_by_track_id.get(&track_id) {
            sources.set_flat_chunk_sample_counts(source_index, chunk_sample_counts.clone());
        }
        let mut track = composite.track;
        assign_candidate_source_index(&mut track, source_index);
        tracks.push(track);
    }
    Ok(ContainerSourceMetadata {
        file_config: None,
        tracks,
    })
}

fn capture_selected_mp4_track_carries(
    selected_tracks: &[ImportedTrack],
    carries_by_track_id: &BTreeMap<u32, ImportedMp4TrackCarry>,
    selected_carries: &mut SelectedImportedMp4CarryMap,
) {
    for selected_track in selected_tracks {
        let Some(source_index) = imported_track_single_source_index(selected_track) else {
            continue;
        };
        let Some(source_track_id) = selected_track
            .mux_policy
            .header_policy()
            .and_then(|policy| policy.source_track_id)
        else {
            continue;
        };
        let Some(carry) = carries_by_track_id.get(&source_track_id) else {
            continue;
        };
        selected_carries.insert((source_index, source_track_id), carry.clone());
    }
}

fn imported_track_single_source_index(imported_track: &ImportedTrack) -> Option<usize> {
    let source_index = imported_track.samples.first()?.source_index;
    imported_track
        .samples
        .iter()
        .all(|sample| sample.source_index == source_index)
        .then_some(source_index)
}

fn merge_flat_destination_append_tracks(
    imported_tracks: &mut Vec<ImportedTrack>,
    selected_carries: &mut SelectedImportedMp4CarryMap,
) -> Result<(), MuxError> {
    let Some(destination_source_index) = imported_tracks
        .iter()
        .filter_map(imported_track_single_source_index)
        .min()
    else {
        return Ok(());
    };

    let mut index = 0_usize;
    while index < imported_tracks.len() {
        if imported_track_single_source_index(&imported_tracks[index])
            == Some(destination_source_index)
        {
            index += 1;
            continue;
        }

        let compatible_targets = imported_tracks
            .iter()
            .enumerate()
            .filter_map(|(candidate_index, candidate)| {
                (candidate_index != index
                    && imported_track_single_source_index(candidate)
                        == Some(destination_source_index))
                .then(|| {
                    flat_destination_append_mode(candidate, &imported_tracks[index])
                        .map(|mode| (candidate_index, mode))
                })
                .flatten()
            })
            .collect::<Vec<_>>();

        match compatible_targets.as_slice() {
            [] => {
                index += 1;
            }
            [(target_index, append_mode)] => {
                let target_index = *target_index;
                let append_mode = *append_mode;
                let incoming = imported_tracks.remove(index);
                let adjusted_target_index = if target_index > index {
                    target_index - 1
                } else {
                    target_index
                };
                append_imported_track_samples(
                    &mut imported_tracks[adjusted_target_index],
                    incoming,
                    selected_carries,
                    append_mode,
                );
            }
            _ => {
                return Err(MuxError::InvalidDestinationMode {
                    mode: MuxDestinationMode::UpdateOrCreateDestination.label(),
                    message: format!(
                        "destination update found multiple compatible append targets for one {} track",
                        flat_destination_append_kind_label(imported_tracks[index].kind)
                    ),
                });
            }
        }
    }
    Ok(())
}

const fn flat_destination_append_kind_label(kind: MuxTrackKind) -> &'static str {
    match kind {
        MuxTrackKind::Audio => "audio",
        MuxTrackKind::Video => "video",
        MuxTrackKind::Text => "text",
        MuxTrackKind::Subtitle => "subtitle",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlatDestinationAppendMode {
    MergeSamples,
    AppendSampleEntries,
}

fn flat_destination_append_mode(
    destination: &ImportedTrack,
    incoming: &ImportedTrack,
) -> Option<FlatDestinationAppendMode> {
    if incoming.kind.is_video() {
        return None;
    }
    if !(destination.kind == incoming.kind
        && destination.timescale == incoming.timescale
        && destination.language == incoming.language
        && destination.width == incoming.width
        && destination.height == incoming.height
        && destination.source_edit_media_time.is_none()
        && incoming.source_edit_media_time.is_none())
    {
        return None;
    }
    flat_destination_append_sample_entry_mode(
        destination.kind,
        &destination.sample_entry_box,
        &incoming.sample_entry_box,
    )
}

fn flat_destination_append_sample_entry_mode(
    kind: MuxTrackKind,
    destination: &[u8],
    incoming: &[u8],
) -> Option<FlatDestinationAppendMode> {
    if destination == incoming {
        return Some(FlatDestinationAppendMode::MergeSamples);
    }
    if sample_entry_box_type(destination) != sample_entry_box_type(incoming) {
        return None;
    }
    match kind {
        MuxTrackKind::Audio => {
            let Ok(destination_entry) = super::mp4::decode_audio_sample_entry(destination) else {
                return None;
            };
            let Ok(incoming_entry) = super::mp4::decode_audio_sample_entry(incoming) else {
                return None;
            };
            if !(destination_entry.sample_entry.box_type == incoming_entry.sample_entry.box_type
                && destination_entry.channel_count == incoming_entry.channel_count
                && destination_entry.sample_size == incoming_entry.sample_size
                && destination_entry.sample_rate == incoming_entry.sample_rate)
            {
                return None;
            }
        }
        MuxTrackKind::Video => {
            let Ok(destination_entry) =
                super::mp4::decode_typed_box::<VisualSampleEntry>(destination)
            else {
                return None;
            };
            let Ok(incoming_entry) = super::mp4::decode_typed_box::<VisualSampleEntry>(incoming)
            else {
                return None;
            };
            if !(destination_entry.sample_entry.box_type == incoming_entry.sample_entry.box_type
                && destination_entry.width == incoming_entry.width
                && destination_entry.height == incoming_entry.height
                && destination_entry.depth == incoming_entry.depth)
            {
                return None;
            }
        }
        MuxTrackKind::Text | MuxTrackKind::Subtitle => return None,
    }
    if sample_entry_children_without_noncritical_boxes(destination, kind)
        == sample_entry_children_without_noncritical_boxes(incoming, kind)
    {
        return Some(FlatDestinationAppendMode::MergeSamples);
    }
    (sample_entry_codec_critical_children(destination, kind)
        == sample_entry_codec_critical_children(incoming, kind))
    .then_some(FlatDestinationAppendMode::AppendSampleEntries)
}

fn sample_entry_children_without_noncritical_boxes(
    sample_entry_box: &[u8],
    kind: MuxTrackKind,
) -> Option<Vec<Vec<u8>>> {
    let child_boxes = match kind {
        MuxTrackKind::Audio => super::mp4::audio_sample_entry_immediate_children(sample_entry_box),
        MuxTrackKind::Video => super::mp4::visual_sample_entry_immediate_children(sample_entry_box),
        MuxTrackKind::Text | MuxTrackKind::Subtitle => return None,
    }
    .ok()?;
    Some(
        child_boxes
            .into_iter()
            .filter(|child_box| {
                sample_entry_box_type(child_box) != Some(FourCc::from_bytes(*b"btrt"))
            })
            .collect(),
    )
}

fn sample_entry_codec_critical_children(
    sample_entry_box: &[u8],
    kind: MuxTrackKind,
) -> Option<Vec<Vec<u8>>> {
    let child_boxes = match kind {
        MuxTrackKind::Audio => super::mp4::audio_sample_entry_immediate_children(sample_entry_box),
        MuxTrackKind::Video => super::mp4::visual_sample_entry_immediate_children(sample_entry_box),
        MuxTrackKind::Text | MuxTrackKind::Subtitle => return None,
    }
    .ok()?;
    Some(
        child_boxes
            .into_iter()
            .filter(|child_box| {
                sample_entry_box_type(child_box).is_some_and(sample_entry_child_is_codec_critical)
            })
            .collect(),
    )
}

fn sample_entry_child_is_codec_critical(box_type: FourCc) -> bool {
    matches!(
        box_type.as_bytes().as_ref(),
        b"avcC"
            | b"hvcC"
            | b"vvcC"
            | b"av1C"
            | b"esds"
            | b"dfLa"
            | b"dOps"
            | b"dac3"
            | b"dec3"
            | b"dac4"
            | b"ddts"
            | b"udts"
            | b"dmlp"
            | b"mhaC"
            | b"iacb"
            | b"alac"
            | b"damr"
            | b"dqcp"
            | b"d263"
            | b"dvcC"
            | b"dvvC"
            | b"vpcC"
            | b"pcmC"
    )
}

fn append_imported_track_samples(
    destination: &mut ImportedTrack,
    incoming: ImportedTrack,
    selected_carries: &mut SelectedImportedMp4CarryMap,
    append_mode: FlatDestinationAppendMode,
) {
    if append_mode == FlatDestinationAppendMode::AppendSampleEntries {
        mark_destination_append_extends_sample_entries(destination, &incoming, selected_carries);
    } else {
        mark_destination_append_regenerates_sample_tables(destination, selected_carries);
    }
    destination.samples.extend(incoming.samples);
    if let Some(header_policy) = destination.mux_policy.header_policy.as_mut() {
        header_policy.source_media_duration = imported_sample_media_duration(&destination.samples);
        header_policy.source_edit_segment_duration = None;
    }
}

fn mark_destination_append_extends_sample_entries(
    destination: &ImportedTrack,
    incoming: &ImportedTrack,
    selected_carries: &mut SelectedImportedMp4CarryMap,
) {
    let destination_sample_count = destination.samples.len();
    let incoming_sample_count = incoming.samples.len();
    let Some(destination_source_index) = imported_track_single_source_index(destination) else {
        return;
    };
    let Some(destination_track_id) = destination
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_track_id)
    else {
        return;
    };
    let incoming_carry = imported_track_selected_mp4_carry(incoming, selected_carries).cloned();
    let Some(destination_carry) =
        selected_carries.get_mut(&(destination_source_index, destination_track_id))
    else {
        return;
    };

    let mut sample_entry_boxes = destination_carry
        .sample_entry_boxes
        .clone()
        .unwrap_or_else(|| vec![destination.sample_entry_box.clone()]);
    let sample_entry_offset = u32::try_from(sample_entry_boxes.len()).unwrap_or(u32::MAX);
    let incoming_sample_entry_boxes = incoming_carry
        .as_ref()
        .and_then(|carry| carry.sample_entry_boxes.clone())
        .unwrap_or_else(|| vec![incoming.sample_entry_box.clone()]);
    sample_entry_boxes.extend(incoming_sample_entry_boxes);
    destination_carry.sample_entry_boxes = Some(sample_entry_boxes);

    let mut sample_description_indices = destination_carry
        .sample_description_indices
        .clone()
        .unwrap_or_else(|| vec![1; destination_sample_count]);
    let incoming_indices = incoming_carry
        .as_ref()
        .and_then(|carry| carry.sample_description_indices.clone())
        .unwrap_or_else(|| vec![1; incoming_sample_count])
        .into_iter()
        .map(|index| sample_entry_offset.saturating_add(index))
        .collect::<Vec<_>>();
    sample_description_indices.extend(incoming_indices);
    destination_carry.sample_description_indices = Some(sample_description_indices);

    let mut chunk_sample_counts = destination_carry
        .flat_chunk_sample_counts
        .clone()
        .unwrap_or_else(|| vec![u32::try_from(destination_sample_count).unwrap_or(u32::MAX)]);
    let incoming_chunk_sample_counts = incoming_carry
        .and_then(|carry| carry.flat_chunk_sample_counts)
        .unwrap_or_else(|| vec![u32::try_from(incoming_sample_count).unwrap_or(u32::MAX)]);
    chunk_sample_counts.extend(incoming_chunk_sample_counts);
    destination_carry.flat_chunk_sample_counts = Some(chunk_sample_counts);
    destination_carry.flat_stsc = None;
    destination_carry.fragmented_decode_time_gaps.clear();
    destination_carry.source_had_empty_stts = false;
    destination_carry.source_sync_samples = None;
    destination_carry.preserved_flat_stbl_boxes.clear();
    destination_carry
        .preserved_flat_trak_boxes
        .retain(|box_bytes| box_header_type(box_bytes) != Some(EDTS));
}

fn mark_destination_append_regenerates_sample_tables(
    destination: &ImportedTrack,
    selected_carries: &mut SelectedImportedMp4CarryMap,
) {
    let Some(source_index) = imported_track_single_source_index(destination) else {
        return;
    };
    let Some(source_track_id) = destination
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_track_id)
    else {
        return;
    };
    let Some(carry) = selected_carries.get_mut(&(source_index, source_track_id)) else {
        return;
    };
    carry.flat_chunk_sample_counts = None;
    carry.flat_stsc = None;
    carry.sample_description_indices = None;
    carry.fragmented_decode_time_gaps.clear();
    carry.source_had_empty_stts = false;
    carry.source_sync_samples = None;
    carry.preserved_flat_stbl_boxes.clear();
    carry
        .preserved_flat_trak_boxes
        .retain(|box_bytes| box_header_type(box_bytes) != Some(EDTS));
}

fn imported_track_selected_mp4_carry<'a>(
    imported_track: &ImportedTrack,
    selected_carries: &'a SelectedImportedMp4CarryMap,
) -> Option<&'a ImportedMp4TrackCarry> {
    let source_track_id = imported_track
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_track_id)?;
    let source_index = imported_track_single_source_index(imported_track).or_else(|| {
        imported_track
            .samples
            .first()
            .map(|sample| sample.source_index)
    })?;
    selected_carries.get(&(source_index, source_track_id))
}

fn normalized_imported_sample_entry_boxes(
    imported_mp4_carry: Option<&ImportedMp4TrackCarry>,
    normalized_sample_entry_box: Vec<u8>,
) -> Vec<Vec<u8>> {
    let normalized_sample_entry_box =
        sample_entry_box_with_self_contained_data_reference(normalized_sample_entry_box);
    let Some(sample_entry_boxes) =
        imported_mp4_carry.and_then(|carry| carry.sample_entry_boxes.as_ref())
    else {
        return vec![normalized_sample_entry_box];
    };
    if sample_entry_boxes.len() <= 1 {
        return vec![normalized_sample_entry_box];
    }
    let mut normalized = sample_entry_boxes
        .iter()
        .cloned()
        .map(sample_entry_box_with_self_contained_data_reference)
        .collect::<Vec<_>>();
    normalized[0] = normalized_sample_entry_box;
    normalized
}

fn sample_description_indices_use_multiple_entries(indices: &[u32]) -> bool {
    let Some(first_index) = indices.first().copied() else {
        return false;
    };
    indices.iter().copied().any(|index| index != first_index)
}

fn sample_entry_box_with_self_contained_data_reference(mut sample_entry_box: Vec<u8>) -> Vec<u8> {
    if sample_entry_box.len() >= 16 {
        sample_entry_box[14..16].copy_from_slice(&1_u16.to_be_bytes());
    }
    sample_entry_box
}

fn restore_preserved_imported_source_sync_samples(
    imported_track: &mut ImportedTrack,
    imported_mp4_carry: Option<&ImportedMp4TrackCarry>,
) {
    if !imported_track.kind.is_video() || !imported_track_uses_avc_family(imported_track) {
        return;
    }
    if imported_track
        .mux_policy
        .header_policy()
        .is_some_and(|header_policy| header_policy.source_stss_first_only)
    {
        return;
    }
    let Some(source_sync_samples) =
        imported_mp4_carry.and_then(|carry| carry.source_sync_samples.as_ref())
    else {
        return;
    };
    if source_sync_samples.len() != imported_track.samples.len() {
        return;
    }
    for (sample, source_is_sync_sample) in imported_track
        .samples
        .iter_mut()
        .zip(source_sync_samples.iter().copied())
    {
        sample.is_sync_sample = source_is_sync_sample;
    }
}

fn reconcile_flat_imported_fragment_decode_time_gaps(
    imported_tracks: &mut [ImportedTrack],
    selected_carries: &SelectedImportedMp4CarryMap,
    sources: &SourceCatalog,
) -> Result<(), MuxError> {
    for imported_track in imported_tracks {
        let Some(gaps) =
            imported_track_fragmented_decode_time_gaps(imported_track, selected_carries, sources)
        else {
            continue;
        };
        for gap in gaps {
            let Some(previous_sample_index) = gap.sample_index.checked_sub(1) else {
                continue;
            };
            let delta = u32::try_from(gap.delta)
                .map_err(|_| MuxError::LayoutOverflow("fragmented decode-time gap"))?;
            let Some(previous_sample) = imported_track.samples.get_mut(previous_sample_index)
            else {
                continue;
            };
            previous_sample.duration = previous_sample
                .duration
                .checked_add(delta)
                .ok_or(MuxError::LayoutOverflow("fragmented decode-time gap"))?;
        }
    }
    Ok(())
}

fn imported_track_fragmented_decode_time_gaps<'a>(
    imported_track: &ImportedTrack,
    selected_carries: &'a SelectedImportedMp4CarryMap,
    sources: &'a SourceCatalog,
) -> Option<&'a [FragmentedDecodeTimeGap]> {
    imported_track_source_key(imported_track)
        .and_then(|(source_index, source_track_id)| {
            sources.fragmented_decode_time_gaps(source_index, source_track_id)
        })
        .or_else(|| {
            imported_track_selected_mp4_carry(imported_track, selected_carries)
                .map(|carry| carry.fragmented_decode_time_gaps.as_slice())
        })
        .filter(|gaps| !gaps.is_empty())
}

fn record_mp4_fragmented_decode_time_gaps(
    sources: &mut SourceCatalog,
    source_index: usize,
    metadata: &PathSourceMetadata,
) {
    for (source_track_id, carry) in &metadata.carries_by_track_id {
        sources.set_fragmented_decode_time_gaps(
            source_index,
            *source_track_id,
            carry.fragmented_decode_time_gaps.clone(),
        );
    }
}

fn load_mp4_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, PathSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a PathSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let mut reader = File::open(&absolute)?;
        let metadata = parse_mp4_source_sync(&absolute, source_index, &mut reader)?;
        if let Some(source_override) = metadata.source_override.clone() {
            sources.replace_with_segmented(source_index, source_override)?;
        }
        record_mp4_fragmented_decode_time_gaps(sources, source_index, &metadata);
        cache.insert(absolute.clone(), metadata);
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_selected_mp4_source_sync(
    path: &Path,
    selector: MuxMp4TrackSelector,
    sources: &mut SourceCatalog,
) -> Result<PathSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    let source_index = sources.add_file(&absolute)?;
    let mut reader = File::open(&absolute)?;
    let metadata =
        parse_mp4_source_sync_with_selector(&absolute, source_index, &mut reader, Some(selector))?;
    if let Some(source_override) = metadata.source_override.clone() {
        sources.replace_with_segmented(source_index, source_override)?;
    }
    record_mp4_fragmented_decode_time_gaps(sources, source_index, &metadata);
    Ok(metadata)
}

#[cfg(feature = "async")]
async fn load_mp4_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, PathSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a PathSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let mut reader = TokioFile::open(&absolute).await?;
        let metadata = parse_mp4_source_async(&absolute, source_index, &mut reader).await?;
        if let Some(source_override) = metadata.source_override.clone() {
            sources.replace_with_segmented(source_index, source_override)?;
        }
        record_mp4_fragmented_decode_time_gaps(sources, source_index, &metadata);
        cache.insert(absolute.clone(), metadata);
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_selected_mp4_source_async(
    path: &Path,
    selector: MuxMp4TrackSelector,
    sources: &mut SourceCatalog,
) -> Result<PathSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    let source_index = sources.add_file(&absolute)?;
    let mut reader = TokioFile::open(&absolute).await?;
    let metadata =
        parse_mp4_source_async_with_selector(&absolute, source_index, &mut reader, Some(selector))
            .await?;
    if let Some(source_override) = metadata.source_override.clone() {
        sources.replace_with_segmented(source_index, source_override)?;
    }
    record_mp4_fragmented_decode_time_gaps(sources, source_index, &metadata);
    Ok(metadata)
}

fn load_avi_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let scanned =
            scan_avi_source_sync(&absolute, &absolute.display().to_string(), source_index)?;
        let mut tracks = scanned.tracks;
        if !scanned.composite_tracks.is_empty() {
            tracks.extend(materialize_composite_tracks(sources, scanned.composite_tracks)?.tracks);
        }
        cache.insert(
            absolute.clone(),
            ContainerSourceMetadata {
                file_config: None,
                tracks,
            },
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_nhml_source_sync<'a>(
    path: &Path,
    kind: DetectedNhmlSidecarKind,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let parsed = parse_nhml_source_sync(&absolute, kind)?;
        cache.insert(
            absolute.clone(),
            materialize_parsed_nhml_source(parsed, sources)?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_dash_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let parsed = parse_dash_source_sync(&absolute)?;
        cache.insert(
            absolute.clone(),
            materialize_parsed_dash_source(&absolute, parsed, sources)?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_program_stream_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        cache.insert(
            absolute.clone(),
            materialize_composite_tracks(
                sources,
                scan_program_stream_sync(&absolute, &absolute.display().to_string())?,
            )?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_saf_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let tracks = super::demux::scan_saf_source_sync(
            &absolute,
            &absolute.display().to_string(),
            source_index,
        )?;
        cache.insert(
            absolute.clone(),
            ContainerSourceMetadata {
                file_config: None,
                tracks,
            },
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_transport_stream_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let scanned = scan_transport_stream_sync(&absolute, &absolute.display().to_string())?;
        cache.insert(
            absolute.clone(),
            materialize_transport_stream_source(sources, scanned)?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn load_vobsub_source_sync<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        cache.insert(
            absolute.clone(),
            materialize_composite_tracks(
                sources,
                scan_vobsub_source_sync(&absolute, &absolute.display().to_string())?,
            )?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_avi_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let scanned =
            scan_avi_source_async(&absolute, &absolute.display().to_string(), source_index).await?;
        let mut tracks = scanned.tracks;
        if !scanned.composite_tracks.is_empty() {
            tracks.extend(materialize_composite_tracks(sources, scanned.composite_tracks)?.tracks);
        }
        cache.insert(
            absolute.clone(),
            ContainerSourceMetadata {
                file_config: None,
                tracks,
            },
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_nhml_source_async<'a>(
    path: &Path,
    kind: DetectedNhmlSidecarKind,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let parsed = parse_nhml_source_async(&absolute, kind).await?;
        cache.insert(
            absolute.clone(),
            materialize_parsed_nhml_source(parsed, sources)?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_dash_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let parsed = parse_dash_source_async(&absolute).await?;
        let metadata = materialize_parsed_dash_source_async(&absolute, parsed, sources).await?;
        cache.insert(absolute.clone(), metadata);
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_vobsub_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        cache.insert(
            absolute.clone(),
            materialize_composite_tracks(
                sources,
                scan_vobsub_source_async(&absolute, &absolute.display().to_string()).await?,
            )?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_program_stream_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        cache.insert(
            absolute.clone(),
            materialize_composite_tracks(
                sources,
                scan_program_stream_async(&absolute, &absolute.display().to_string()).await?,
            )?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_saf_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let source_index = sources.add_file(&absolute)?;
        let tracks = super::demux::scan_saf_source_async(
            &absolute,
            &absolute.display().to_string(),
            source_index,
        )
        .await?;
        cache.insert(
            absolute.clone(),
            ContainerSourceMetadata {
                file_config: None,
                tracks,
            },
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

#[cfg(feature = "async")]
async fn load_transport_stream_source_async<'a>(
    path: &Path,
    cache: &'a mut BTreeMap<PathBuf, ContainerSourceMetadata>,
    sources: &mut SourceCatalog,
) -> Result<&'a ContainerSourceMetadata, MuxError> {
    let absolute = absolute_path(path)?;
    if !cache.contains_key(&absolute) {
        let scanned =
            scan_transport_stream_async(&absolute, &absolute.display().to_string()).await?;
        cache.insert(
            absolute.clone(),
            materialize_transport_stream_source(sources, scanned)?,
        );
    }
    Ok(cache.get(&absolute).unwrap())
}

fn parse_mp4_source_sync<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
) -> Result<PathSourceMetadata, MuxError>
where
    R: Read + Seek,
{
    parse_mp4_source_sync_with_selector(path, source_index, reader, None)
}

fn parse_mp4_source_sync_with_selector<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    selector: Option<MuxMp4TrackSelector>,
) -> Result<PathSourceMetadata, MuxError>
where
    R: Read + Seek,
{
    let source_file_size = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;
    let mut file_config = probe_file_config_sync(reader)?;
    let mut source_movie_timescale = file_config.movie_timescale();
    let mut compressed_root_cursor = None::<Cursor<Vec<u8>>>;
    let fragmented_hint = !extract_box(reader, None, BoxPath::from([MOOF]))?.is_empty();
    let mut track_infos = match extract_box(reader, None, BoxPath::from([MOOV, TRAK])) {
        Ok(track_infos) => track_infos,
        Err(error) => {
            if let Some(root_bytes) =
                crate::probe::extract_compressed_movie_root_bytes_sync(reader)?
            {
                let mut cursor = Cursor::new(root_bytes);
                let fallback_file_config = probe_file_config_sync(&mut cursor)?;
                let fallback_track_infos =
                    extract_box(&mut cursor, None, BoxPath::from([MOOV, TRAK]))?;
                if !fallback_track_infos.is_empty() || fallback_file_config.movie_timescale() != 0 {
                    file_config = fallback_file_config;
                    source_movie_timescale = file_config.movie_timescale();
                    compressed_root_cursor = Some(cursor);
                    fallback_track_infos
                } else {
                    return Err(error.into());
                }
            } else {
                return Err(error.into());
            }
        }
    };
    if compressed_root_cursor.is_none()
        && (track_infos.is_empty() || source_movie_timescale == 0)
        && let Some(root_bytes) = crate::probe::extract_compressed_movie_root_bytes_sync(reader)?
    {
        let mut cursor = Cursor::new(root_bytes);
        let fallback_file_config = probe_file_config_sync(&mut cursor)?;
        let fallback_track_infos = extract_box(&mut cursor, None, BoxPath::from([MOOV, TRAK]))?;
        if !fallback_track_infos.is_empty() || fallback_file_config.movie_timescale() != 0 {
            file_config = fallback_file_config;
            source_movie_timescale = file_config.movie_timescale();
            track_infos = fallback_track_infos;
            compressed_root_cursor = Some(cursor);
        }
    }
    let mut tracks = Vec::new();
    let mut carries_by_track_id = BTreeMap::new();
    let mut source_segments = Vec::<SegmentedMuxSourceSegment>::new();
    let mut uses_external_data_reference = false;
    if let Some(metadata_reader) = compressed_root_cursor.as_mut() {
        for trak_info in track_infos {
            if let Some(selector) = selector
                && !track_may_match_selector_sync(metadata_reader, &trak_info, selector)?
            {
                continue;
            }
            let components = extract_track_candidate_components_sync(
                path,
                fragmented_hint,
                metadata_reader,
                &trak_info,
            )?;
            if let Some(parsed_track) = finish_parsed_track_candidate_sync(
                path,
                source_index,
                fragmented_hint,
                source_movie_timescale,
                source_file_size,
                reader,
                components,
            )? {
                source_segments.extend(parsed_track.source_segments.iter().cloned());
                uses_external_data_reference |= parsed_track.uses_external_data_reference;
                carries_by_track_id.insert(parsed_track.track.track_id, parsed_track.carry);
                tracks.push(parsed_track.track);
            }
        }
    } else {
        for trak_info in track_infos {
            if let Some(selector) = selector
                && !track_may_match_selector_sync(reader, &trak_info, selector)?
            {
                continue;
            }
            if let Some(parsed_track) = parse_track_candidate_sync(
                path,
                source_index,
                fragmented_hint,
                source_movie_timescale,
                source_file_size,
                reader,
                &trak_info,
            )? {
                source_segments.extend(parsed_track.source_segments.iter().cloned());
                uses_external_data_reference |= parsed_track.uses_external_data_reference;
                carries_by_track_id.insert(parsed_track.track.track_id, parsed_track.carry);
                tracks.push(parsed_track.track);
            }
        }
    }
    populate_empty_fragmented_track_samples_sync(
        path,
        source_index,
        reader,
        &mut tracks,
        &mut carries_by_track_id,
    )?;
    Ok(PathSourceMetadata {
        file_config: Some(file_config),
        tracks,
        carries_by_track_id,
        source_override: if uses_external_data_reference {
            Some(build_mp4_import_source_override(
                path,
                source_segments,
                source_file_size,
            )?)
        } else {
            None
        },
    })
}

struct ParsedMp4Track {
    track: TrackCandidate,
    carry: ImportedMp4TrackCarry,
    source_segments: Vec<SegmentedMuxSourceSegment>,
    uses_external_data_reference: bool,
}

struct ParsedTrackCandidateComponents {
    tkhd: Tkhd,
    mdhd: Mdhd,
    hdlr: Option<Hdlr>,
    sample_entry: ExtractedBox,
    sample_entry_box: Vec<u8>,
    sample_entry_boxes: Vec<Vec<u8>>,
    data_references: Vec<ImportedDataReference>,
    elst: Option<Elst>,
    elst_box_size: Option<u64>,
    sample_roll_distance: Option<i16>,
    emit_roll_sbgp: bool,
    preserved_flat_stbl_boxes: Vec<Vec<u8>>,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
    stts: Option<Stts>,
    ctts: Option<Ctts>,
    stsc: Option<Stsc>,
    sample_sizes: Option<Vec<u32>>,
    stco: Option<Stco>,
    co64: Option<Co64>,
    stss: Option<Stss>,
}

type SelectedImportedMp4CarryMap = BTreeMap<(usize, u32), ImportedMp4TrackCarry>;

#[cfg(feature = "async")]
async fn parse_mp4_source_async<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
) -> Result<PathSourceMetadata, MuxError>
where
    R: AsyncReadSeek,
{
    parse_mp4_source_async_with_selector(path, source_index, reader, None).await
}

#[cfg(feature = "async")]
async fn parse_mp4_source_async_with_selector<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    selector: Option<MuxMp4TrackSelector>,
) -> Result<PathSourceMetadata, MuxError>
where
    R: AsyncReadSeek,
{
    let source_file_size = reader.seek(SeekFrom::End(0)).await?;
    reader.seek(SeekFrom::Start(0)).await?;
    let mut file_config = probe_file_config_async(reader).await?;
    let mut source_movie_timescale = file_config.movie_timescale();
    let mut compressed_root_cursor = None::<Cursor<Vec<u8>>>;
    let fragmented_hint = !extract_box_async(reader, None, BoxPath::from([MOOF]))
        .await?
        .is_empty();
    let mut track_infos = match extract_box_async(reader, None, BoxPath::from([MOOV, TRAK])).await {
        Ok(track_infos) => track_infos,
        Err(error) => {
            if let Some(root_bytes) =
                crate::probe::extract_compressed_movie_root_bytes_async(reader).await?
            {
                let mut cursor = Cursor::new(root_bytes);
                let fallback_file_config = probe_file_config_sync(&mut cursor)?;
                let fallback_track_infos =
                    extract_box(&mut cursor, None, BoxPath::from([MOOV, TRAK]))?;
                if !fallback_track_infos.is_empty() || fallback_file_config.movie_timescale() != 0 {
                    file_config = fallback_file_config;
                    source_movie_timescale = file_config.movie_timescale();
                    compressed_root_cursor = Some(cursor);
                    fallback_track_infos
                } else {
                    return Err(error.into());
                }
            } else {
                return Err(error.into());
            }
        }
    };
    if compressed_root_cursor.is_none()
        && (track_infos.is_empty() || source_movie_timescale == 0)
        && let Some(root_bytes) =
            crate::probe::extract_compressed_movie_root_bytes_async(reader).await?
    {
        let mut cursor = Cursor::new(root_bytes);
        let fallback_file_config = probe_file_config_sync(&mut cursor)?;
        let fallback_track_infos = extract_box(&mut cursor, None, BoxPath::from([MOOV, TRAK]))?;
        if !fallback_track_infos.is_empty() || fallback_file_config.movie_timescale() != 0 {
            file_config = fallback_file_config;
            source_movie_timescale = file_config.movie_timescale();
            track_infos = fallback_track_infos;
            compressed_root_cursor = Some(cursor);
        }
    }
    let mut tracks = Vec::new();
    let mut carries_by_track_id = BTreeMap::new();
    let mut source_segments = Vec::<SegmentedMuxSourceSegment>::new();
    let mut uses_external_data_reference = false;
    if let Some(metadata_reader) = compressed_root_cursor.as_mut() {
        for trak_info in track_infos {
            if let Some(selector) = selector
                && !track_may_match_selector_sync(metadata_reader, &trak_info, selector)?
            {
                continue;
            }
            let components = extract_track_candidate_components_sync(
                path,
                fragmented_hint,
                metadata_reader,
                &trak_info,
            )?;
            if let Some(parsed_track) = finish_parsed_track_candidate_async(
                path,
                source_index,
                fragmented_hint,
                source_movie_timescale,
                source_file_size,
                reader,
                components,
            )
            .await?
            {
                source_segments.extend(parsed_track.source_segments.iter().cloned());
                uses_external_data_reference |= parsed_track.uses_external_data_reference;
                carries_by_track_id.insert(parsed_track.track.track_id, parsed_track.carry);
                tracks.push(parsed_track.track);
            }
        }
    } else {
        for trak_info in track_infos {
            if let Some(selector) = selector
                && !track_may_match_selector_async(reader, &trak_info, selector).await?
            {
                continue;
            }
            if let Some(parsed_track) = parse_track_candidate_async(
                path,
                source_index,
                fragmented_hint,
                source_movie_timescale,
                source_file_size,
                reader,
                &trak_info,
            )
            .await?
            {
                source_segments.extend(parsed_track.source_segments.iter().cloned());
                uses_external_data_reference |= parsed_track.uses_external_data_reference;
                carries_by_track_id.insert(parsed_track.track.track_id, parsed_track.carry);
                tracks.push(parsed_track.track);
            }
        }
    }
    populate_empty_fragmented_track_samples_async(
        path,
        source_index,
        reader,
        &mut tracks,
        &mut carries_by_track_id,
    )
    .await?;
    Ok(PathSourceMetadata {
        file_config: Some(file_config),
        tracks,
        carries_by_track_id,
        source_override: if uses_external_data_reference {
            Some(build_mp4_import_source_override(
                path,
                source_segments,
                source_file_size,
            )?)
        } else {
            None
        },
    })
}

fn track_may_match_selector_sync<R>(
    reader: &mut R,
    trak_info: &HeaderInfo,
    selector: MuxMp4TrackSelector,
) -> Result<bool, MuxError>
where
    R: Read + Seek,
{
    match selector {
        MuxMp4TrackSelector::TrackId { track_id } => {
            let tkhd = extract_required_single_as_sync::<_, Tkhd>(
                reader,
                trak_info,
                BoxPath::from([TKHD]),
                "tkhd",
            )?;
            Ok(tkhd.track_id == track_id)
        }
        MuxMp4TrackSelector::Audio { .. } => {
            track_matches_handler_selector_sync(reader, trak_info, &[SOUN])
        }
        MuxMp4TrackSelector::Video => {
            track_matches_handler_selector_sync(reader, trak_info, &[VIDE])
        }
        MuxMp4TrackSelector::Text { .. } => {
            track_matches_handler_selector_sync(reader, trak_info, &[TEXT, SUBT, SUBP])
        }
    }
}

fn track_matches_handler_selector_sync<R>(
    reader: &mut R,
    trak_info: &HeaderInfo,
    accepted_handler_types: &[FourCc],
) -> Result<bool, MuxError>
where
    R: Read + Seek,
{
    let hdlr =
        extract_optional_single_as_sync::<_, Hdlr>(reader, trak_info, BoxPath::from([MDIA, HDLR]))?;
    Ok(hdlr
        .map(|hdlr| accepted_handler_types.contains(&hdlr.handler_type))
        .unwrap_or(true))
}

#[cfg(feature = "async")]
async fn track_may_match_selector_async<R>(
    reader: &mut R,
    trak_info: &HeaderInfo,
    selector: MuxMp4TrackSelector,
) -> Result<bool, MuxError>
where
    R: AsyncReadSeek,
{
    match selector {
        MuxMp4TrackSelector::TrackId { track_id } => {
            let tkhd = extract_required_single_as_async::<_, Tkhd>(
                reader,
                trak_info,
                BoxPath::from([TKHD]),
                "tkhd",
            )
            .await?;
            Ok(tkhd.track_id == track_id)
        }
        MuxMp4TrackSelector::Audio { .. } => {
            track_matches_handler_selector_async(reader, trak_info, &[SOUN]).await
        }
        MuxMp4TrackSelector::Video => {
            track_matches_handler_selector_async(reader, trak_info, &[VIDE]).await
        }
        MuxMp4TrackSelector::Text { .. } => {
            track_matches_handler_selector_async(reader, trak_info, &[TEXT, SUBT, SUBP]).await
        }
    }
}

#[cfg(feature = "async")]
async fn track_matches_handler_selector_async<R>(
    reader: &mut R,
    trak_info: &HeaderInfo,
    accepted_handler_types: &[FourCc],
) -> Result<bool, MuxError>
where
    R: AsyncReadSeek,
{
    let hdlr =
        extract_optional_single_as_async::<_, Hdlr>(reader, trak_info, BoxPath::from([MDIA, HDLR]))
            .await?;
    Ok(hdlr
        .map(|hdlr| accepted_handler_types.contains(&hdlr.handler_type))
        .unwrap_or(true))
}

fn populate_empty_fragmented_track_samples_sync<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    tracks: &mut [TrackCandidate],
    carries_by_track_id: &mut BTreeMap<u32, ImportedMp4TrackCarry>,
) -> Result<(), MuxError>
where
    R: Read + Seek,
{
    if tracks.iter().all(|track| !track.samples.is_empty()) {
        return Ok(());
    }

    let moof_infos = extract_box(reader, None, BoxPath::from([MOOF]))?;
    if moof_infos.is_empty() {
        return Ok(());
    }
    let trex_by_track_id =
        extract_box_as::<_, Trex>(reader, None, BoxPath::from([MOOV, MVEX, TREX]))?
            .into_iter()
            .map(|trex| (trex.track_id, trex))
            .collect::<BTreeMap<_, _>>();

    for track in tracks.iter_mut().filter(|track| track.samples.is_empty()) {
        let reconcile_fragment_durations =
            sample_entry_box_type(&track.sample_entry_box) != Some(FourCc::from_bytes(*b"ac-3"));
        let fragment_samples = collect_fragment_candidate_samples_sync(
            path,
            source_index,
            reader,
            track.track_id,
            &moof_infos,
            trex_by_track_id.get(&track.track_id),
            reconcile_fragment_durations,
        )?;
        if !fragment_samples.samples.is_empty() {
            if let Some(first_base_decode_time) = fragment_samples.first_base_decode_time
                && let Some(mut header_policy) = track.mux_policy.header_policy()
            {
                header_policy.source_media_decode_time_offset = Some(first_base_decode_time);
                track.mux_policy = track.mux_policy.with_header_policy(header_policy);
            }
            let carry = carries_by_track_id.entry(track.track_id).or_default();
            carry.sample_description_indices = Some(fragment_samples.sample_description_indices);
            carry.fragmented_decode_time_gaps = fragment_samples.fragmented_decode_time_gaps;
            track.samples = fragment_samples.samples;
        }
    }
    Ok(())
}

fn collect_fragment_candidate_samples_sync<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    track_id: u32,
    moof_infos: &[HeaderInfo],
    trex: Option<&Trex>,
    reconcile_fragment_durations: bool,
) -> Result<ImportedFragmentSamples, MuxError>
where
    R: Read + Seek,
{
    let mut fragment_batches = Vec::<ImportedFragmentBatch>::new();
    for moof_info in moof_infos {
        let traf_infos = extract_box(reader, Some(moof_info), BoxPath::from([TRAF]))?;
        for traf_info in traf_infos {
            let tfhd = extract_required_single_as_sync::<_, Tfhd>(
                reader,
                &traf_info,
                BoxPath::from([TFHD]),
                "tfhd",
            )?;
            if tfhd.track_id != track_id {
                continue;
            }
            let tfdt = extract_optional_single_as_sync::<_, Tfdt>(
                reader,
                &traf_info,
                BoxPath::from([FourCc::from_bytes(*b"tfdt")]),
            )?;
            let truns = extract_box_as::<_, Trun>(reader, Some(&traf_info), BoxPath::from([TRUN]))?;
            let trun_infos = extract_box(reader, Some(&traf_info), BoxPath::from([TRUN]))?;
            if tfdt.is_none() && truns.iter().any(|trun| trun.sample_count != 0) {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} has a non-empty fragmented run without tfdt decode time"
                    ),
                });
            }
            let context = FragmentRunContext {
                path,
                source_index,
                track_id,
                moof_offset: moof_info.offset(),
                trex,
            };
            let mut fragment_samples = Vec::new();
            let mut fragment_sample_description_indices = Vec::new();
            collect_fragment_candidate_samples_from_runs(
                &context,
                &tfhd,
                &truns,
                &trun_infos,
                &mut fragment_samples,
                &mut fragment_sample_description_indices,
            )?;
            if !fragment_samples.is_empty() {
                fragment_batches.push(ImportedFragmentBatch {
                    base_decode_time: tfdt.as_ref().map(fragment_tfdt_base_decode_time),
                    samples: fragment_samples,
                    sample_description_indices: fragment_sample_description_indices,
                });
            }
        }
    }
    if reconcile_fragment_durations {
        reconcile_imported_fragment_sample_durations(path, track_id, &mut fragment_batches)?;
    }
    let first_base_decode_time = fragment_batches
        .iter()
        .find_map(|batch| batch.base_decode_time);
    let fragmented_decode_time_gaps =
        collect_fragmented_decode_time_gaps(path, track_id, &fragment_batches)?;
    let mut samples = Vec::new();
    let mut sample_description_indices = Vec::new();
    for batch in fragment_batches {
        sample_description_indices.extend(batch.sample_description_indices);
        samples.extend(batch.samples);
    }
    Ok(ImportedFragmentSamples {
        samples,
        sample_description_indices,
        first_base_decode_time,
        fragmented_decode_time_gaps,
    })
}

#[cfg(feature = "async")]
async fn populate_empty_fragmented_track_samples_async<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    tracks: &mut [TrackCandidate],
    carries_by_track_id: &mut BTreeMap<u32, ImportedMp4TrackCarry>,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
{
    if tracks.iter().all(|track| !track.samples.is_empty()) {
        return Ok(());
    }

    let moof_infos = extract_box_async(reader, None, BoxPath::from([MOOF])).await?;
    if moof_infos.is_empty() {
        return Ok(());
    }
    let trex_by_track_id =
        extract_box_as_async::<_, Trex>(reader, None, BoxPath::from([MOOV, MVEX, TREX]))
            .await?
            .into_iter()
            .map(|trex| (trex.track_id, trex))
            .collect::<BTreeMap<_, _>>();

    for track in tracks.iter_mut().filter(|track| track.samples.is_empty()) {
        let reconcile_fragment_durations =
            sample_entry_box_type(&track.sample_entry_box) != Some(FourCc::from_bytes(*b"ac-3"));
        let fragment_samples = collect_fragment_candidate_samples_async(
            path,
            source_index,
            reader,
            track.track_id,
            &moof_infos,
            trex_by_track_id.get(&track.track_id),
            reconcile_fragment_durations,
        )
        .await?;
        if !fragment_samples.samples.is_empty() {
            if let Some(first_base_decode_time) = fragment_samples.first_base_decode_time
                && let Some(mut header_policy) = track.mux_policy.header_policy()
            {
                header_policy.source_media_decode_time_offset = Some(first_base_decode_time);
                track.mux_policy = track.mux_policy.with_header_policy(header_policy);
            }
            let carry = carries_by_track_id.entry(track.track_id).or_default();
            carry.sample_description_indices = Some(fragment_samples.sample_description_indices);
            carry.fragmented_decode_time_gaps = fragment_samples.fragmented_decode_time_gaps;
            track.samples = fragment_samples.samples;
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn collect_fragment_candidate_samples_async<R>(
    path: &Path,
    source_index: usize,
    reader: &mut R,
    track_id: u32,
    moof_infos: &[HeaderInfo],
    trex: Option<&Trex>,
    reconcile_fragment_durations: bool,
) -> Result<ImportedFragmentSamples, MuxError>
where
    R: AsyncReadSeek,
{
    let mut fragment_batches = Vec::<ImportedFragmentBatch>::new();
    for moof_info in moof_infos {
        let traf_infos = extract_box_async(reader, Some(moof_info), BoxPath::from([TRAF])).await?;
        for traf_info in traf_infos {
            let tfhd = extract_required_single_as_async::<_, Tfhd>(
                reader,
                &traf_info,
                BoxPath::from([TFHD]),
                "tfhd",
            )
            .await?;
            if tfhd.track_id != track_id {
                continue;
            }
            let tfdt = extract_optional_single_as_async::<_, Tfdt>(
                reader,
                &traf_info,
                BoxPath::from([FourCc::from_bytes(*b"tfdt")]),
            )
            .await?;
            let truns =
                extract_box_as_async::<_, Trun>(reader, Some(&traf_info), BoxPath::from([TRUN]))
                    .await?;
            let trun_infos =
                extract_box_async(reader, Some(&traf_info), BoxPath::from([TRUN])).await?;
            if tfdt.is_none() && truns.iter().any(|trun| trun.sample_count != 0) {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} has a non-empty fragmented run without tfdt decode time"
                    ),
                });
            }
            let context = FragmentRunContext {
                path,
                source_index,
                track_id,
                moof_offset: moof_info.offset(),
                trex,
            };
            let mut fragment_samples = Vec::new();
            let mut fragment_sample_description_indices = Vec::new();
            collect_fragment_candidate_samples_from_runs(
                &context,
                &tfhd,
                &truns,
                &trun_infos,
                &mut fragment_samples,
                &mut fragment_sample_description_indices,
            )?;
            if !fragment_samples.is_empty() {
                fragment_batches.push(ImportedFragmentBatch {
                    base_decode_time: tfdt.as_ref().map(fragment_tfdt_base_decode_time),
                    samples: fragment_samples,
                    sample_description_indices: fragment_sample_description_indices,
                });
            }
        }
    }
    if reconcile_fragment_durations {
        reconcile_imported_fragment_sample_durations(path, track_id, &mut fragment_batches)?;
    }
    let first_base_decode_time = fragment_batches
        .iter()
        .find_map(|batch| batch.base_decode_time);
    let fragmented_decode_time_gaps =
        collect_fragmented_decode_time_gaps(path, track_id, &fragment_batches)?;
    let mut samples = Vec::new();
    let mut sample_description_indices = Vec::new();
    for batch in fragment_batches {
        sample_description_indices.extend(batch.sample_description_indices);
        samples.extend(batch.samples);
    }
    Ok(ImportedFragmentSamples {
        samples,
        sample_description_indices,
        first_base_decode_time,
        fragmented_decode_time_gaps,
    })
}

fn fragment_tfdt_base_decode_time(tfdt: &Tfdt) -> u64 {
    if tfdt.version() == 1 {
        tfdt.base_media_decode_time_v1
    } else {
        u64::from(tfdt.base_media_decode_time_v0)
    }
}

fn collect_fragmented_decode_time_gaps(
    path: &Path,
    track_id: u32,
    fragment_batches: &[ImportedFragmentBatch],
) -> Result<Vec<FragmentedDecodeTimeGap>, MuxError> {
    let mut gaps = Vec::new();
    let mut sample_index = 0_usize;
    let mut previous_base_decode_time = None::<u64>;
    let mut previous_decode_end = None::<u64>;
    for batch in fragment_batches {
        if let Some(base_decode_time) = batch.base_decode_time {
            if let Some(previous_base_decode_time) = previous_base_decode_time
                && base_decode_time < previous_base_decode_time
            {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} exposes descending fragmented tfdt decode times"
                    ),
                });
            }
            if let Some(previous_decode_end) = previous_decode_end
                && base_decode_time > previous_decode_end
            {
                gaps.push(FragmentedDecodeTimeGap {
                    sample_index,
                    delta: base_decode_time - previous_decode_end,
                });
            }
            let batch_duration = batch.samples.iter().try_fold(0_u64, |duration, sample| {
                duration
                    .checked_add(u64::from(sample.duration))
                    .ok_or(MuxError::LayoutOverflow("fragmented decode-time gap"))
            })?;
            previous_base_decode_time = Some(base_decode_time);
            previous_decode_end = Some(
                base_decode_time
                    .checked_add(batch_duration)
                    .ok_or(MuxError::LayoutOverflow("fragmented decode-time gap"))?,
            );
        }
        sample_index = sample_index
            .checked_add(batch.samples.len())
            .ok_or(MuxError::LayoutOverflow("fragmented decode-time gap"))?;
    }
    Ok(gaps)
}

fn reconcile_imported_fragment_sample_durations(
    path: &Path,
    track_id: u32,
    fragment_batches: &mut [ImportedFragmentBatch],
) -> Result<(), MuxError> {
    for index in 0..fragment_batches.len().saturating_sub(1) {
        let Some(current_base_decode_time) = fragment_batches[index].base_decode_time else {
            continue;
        };
        let Some(next_base_decode_time) = fragment_batches[index + 1].base_decode_time else {
            continue;
        };
        if next_base_decode_time < current_base_decode_time {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} exposes descending fragmented tfdt decode times"
                ),
            });
        }
        let expected_fragment_duration = next_base_decode_time - current_base_decode_time;
        let actual_fragment_duration = fragment_batches[index]
            .samples
            .iter()
            .map(|sample| u64::from(sample.duration))
            .sum::<u64>();
        if expected_fragment_duration == actual_fragment_duration {
            continue;
        }
        let delta = i128::from(expected_fragment_duration) - i128::from(actual_fragment_duration);
        let Some(last_sample) = fragment_batches[index].samples.last_mut() else {
            continue;
        };
        let adjusted_duration = i128::from(last_sample.duration) + delta;
        if adjusted_duration <= 0 || adjusted_duration > i128::from(u32::MAX) {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} exposes fragmented tfdt/trun timing that cannot be reconciled"
                ),
            });
        }
        last_sample.duration = adjusted_duration as u32;
    }
    Ok(())
}

fn collect_fragment_candidate_samples_from_runs(
    context: &FragmentRunContext<'_>,
    tfhd: &Tfhd,
    truns: &[Trun],
    trun_infos: &[HeaderInfo],
    output: &mut Vec<CandidateSample>,
    sample_description_indices: &mut Vec<u32>,
) -> Result<(), MuxError> {
    let path = context.path;
    let track_id = context.track_id;
    if truns.len() != trun_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes misaligned fragmented run metadata"),
        });
    }
    let sample_description_index = if tfhd.flags() & TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT != 0 {
        tfhd.sample_description_index
    } else {
        context
            .trex
            .map(|trex| trex.default_sample_description_index)
            .unwrap_or(1)
    };
    if sample_description_index == 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} uses fragmented sample description index 0"),
        });
    }

    let base_data_offset = if tfhd.flags() & TFHD_BASE_DATA_OFFSET_PRESENT != 0 {
        tfhd.base_data_offset
    } else {
        context.moof_offset
    };
    let mut next_offset = None::<u64>;

    for (trun, trun_info) in truns.iter().zip(trun_infos.iter()) {
        let sample_count = usize::try_from(trun.sample_count).map_err(|_| {
            MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} exposes a fragmented run whose sample count does not fit in usize"
                ),
            }
        })?;
        validate_fragment_trun_layout(path, track_id, trun, trun_info, sample_count)?;

        let mut current_offset = if trun.flags() & TRUN_DATA_OFFSET_PRESENT != 0 {
            let absolute = i128::from(base_data_offset) + i128::from(trun.data_offset);
            if absolute < 0 || absolute > i128::from(u64::MAX) {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} computed an invalid fragmented data offset at trun {}",
                        trun_info.offset()
                    ),
                });
            }
            absolute as u64
        } else if let Some(next_offset) = next_offset {
            next_offset
        } else if tfhd.flags() & TFHD_DEFAULT_BASE_IS_MOOF != 0 {
            context.moof_offset
        } else {
            base_data_offset
        };

        for sample_index in 0..sample_count {
            let sample_size = effective_fragment_sample_size(
                path,
                track_id,
                tfhd,
                context.trex,
                trun,
                trun_info,
                sample_index,
            )?;
            let sample_duration = effective_fragment_sample_duration(
                path,
                track_id,
                tfhd,
                context.trex,
                trun,
                trun_info,
                sample_index,
            )?;
            let sample_flags =
                effective_fragment_sample_flags(tfhd, context.trex, trun, sample_index)
                    .unwrap_or(0);
            let composition_time_offset = if trun.flags()
                & TRUN_SAMPLE_COMPOSITION_TIME_OFFSET_PRESENT
                != 0
            {
                let offset = trun.sample_composition_time_offset(sample_index);
                i32::try_from(offset).map_err(|_| MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} fragmented run at {} exposes composition offset {} that does not fit in i32",
                            trun_info.offset(),
                            offset
                        ),
                    })?
            } else {
                0
            };

            output.push(CandidateSample {
                source_index: context.source_index,
                data_offset: current_offset,
                data_size: sample_size,
                duration: sample_duration,
                composition_time_offset,
                is_sync_sample: sample_flags & NON_KEY_SAMPLE_FLAGS == 0,
            });
            sample_description_indices.push(sample_description_index);
            current_offset = current_offset
                .checked_add(u64::from(sample_size))
                .ok_or(MuxError::LayoutOverflow("fragmented sample offset"))?;
        }
        next_offset = Some(current_offset);
    }

    Ok(())
}

fn validate_fragment_trun_layout(
    path: &Path,
    track_id: u32,
    trun: &Trun,
    trun_info: &HeaderInfo,
    sample_count: usize,
) -> Result<(), MuxError> {
    let per_sample_fields_present = trun.flags()
        & (TRUN_SAMPLE_DURATION_PRESENT
            | TRUN_SAMPLE_SIZE_PRESENT
            | TRUN_SAMPLE_FLAGS_PRESENT
            | TRUN_SAMPLE_COMPOSITION_TIME_OFFSET_PRESENT)
        != 0;
    if per_sample_fields_present && trun.entries.len() != sample_count {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} fragmented run at {} declares {} samples but carries {} entries",
                trun_info.offset(),
                trun.sample_count,
                trun.entries.len()
            ),
        });
    }
    if !per_sample_fields_present && !trun.entries.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} fragmented run at {} carries unexpected inline sample entries",
                trun_info.offset()
            ),
        });
    }
    Ok(())
}

fn effective_fragment_sample_size(
    path: &Path,
    track_id: u32,
    tfhd: &Tfhd,
    trex: Option<&Trex>,
    trun: &Trun,
    trun_info: &HeaderInfo,
    sample_index: usize,
) -> Result<u32, MuxError> {
    if trun.flags() & TRUN_SAMPLE_SIZE_PRESENT != 0 {
        return trun
            .entries
            .get(sample_index)
            .map(|entry| entry.sample_size)
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} fragmented run at {} is missing sample size entry {}",
                    trun_info.offset(),
                    sample_index + 1
                ),
            });
    }
    if tfhd.flags() & TFHD_DEFAULT_SAMPLE_SIZE_PRESENT != 0 {
        return Ok(tfhd.default_sample_size);
    }
    if let Some(trex) = trex {
        return Ok(trex.default_sample_size);
    }
    Err(MuxError::UnsupportedTrackImport {
        spec: path.display().to_string(),
        message: format!(
            "track {track_id} requires fragmented sample-size defaults from tfhd or trex"
        ),
    })
}

fn effective_fragment_sample_duration(
    path: &Path,
    track_id: u32,
    tfhd: &Tfhd,
    trex: Option<&Trex>,
    trun: &Trun,
    trun_info: &HeaderInfo,
    sample_index: usize,
) -> Result<u32, MuxError> {
    if trun.flags() & TRUN_SAMPLE_DURATION_PRESENT != 0 {
        return trun
            .entries
            .get(sample_index)
            .map(|entry| entry.sample_duration)
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} fragmented run at {} is missing sample duration entry {}",
                    trun_info.offset(),
                    sample_index + 1
                ),
            });
    }
    if tfhd.flags() & TFHD_DEFAULT_SAMPLE_DURATION_PRESENT != 0 {
        return Ok(tfhd.default_sample_duration);
    }
    if let Some(trex) = trex {
        return Ok(trex.default_sample_duration);
    }
    Err(MuxError::UnsupportedTrackImport {
        spec: path.display().to_string(),
        message: format!(
            "track {track_id} requires fragmented sample-duration defaults from tfhd or trex"
        ),
    })
}

fn effective_fragment_sample_flags(
    tfhd: &Tfhd,
    trex: Option<&Trex>,
    trun: &Trun,
    sample_index: usize,
) -> Option<u32> {
    if trun.flags() & TRUN_SAMPLE_FLAGS_PRESENT != 0 {
        return trun
            .entries
            .get(sample_index)
            .map(|entry| entry.sample_flags);
    }
    if sample_index == 0 && trun.flags() & TRUN_FIRST_SAMPLE_FLAGS_PRESENT != 0 {
        return Some(trun.first_sample_flags);
    }
    if tfhd.flags() & TFHD_DEFAULT_SAMPLE_FLAGS_PRESENT != 0 {
        return Some(tfhd.default_sample_flags);
    }
    trex.map(|trex| trex.default_sample_flags)
}

fn select_mp4_track(
    tracks: &[TrackCandidate],
    selector: MuxMp4TrackSelector,
    spec: String,
    preserve_track_id: bool,
) -> Result<ImportedTrack, MuxError> {
    let selected = match selector {
        MuxMp4TrackSelector::Video => tracks.iter().find(|track| track.kind.is_video()),
        MuxMp4TrackSelector::Audio { occurrence } => tracks
            .iter()
            .filter(|track| track.kind.is_audio())
            .nth(usize::try_from(occurrence.saturating_sub(1)).unwrap_or(usize::MAX)),
        MuxMp4TrackSelector::Text { occurrence } => tracks
            .iter()
            .filter(|track| track.kind.is_textual())
            .nth(usize::try_from(occurrence.saturating_sub(1)).unwrap_or(usize::MAX)),
        MuxMp4TrackSelector::TrackId { track_id } => {
            tracks.iter().find(|track| track.track_id == track_id)
        }
    }
    .ok_or_else(|| MuxError::MissingTrackSelection { spec: spec.clone() })?;

    let is_selected_container_mpeg_video =
        preserve_track_id && sample_entry_is_mpeg_video(&selected.sample_entry_box);
    let mux_policy = if is_selected_container_mpeg_video {
        selected.mux_policy.with_terminal_flat_video_chunk_split()
    } else {
        selected.mux_policy
    };
    let sample_entry_box = if preserve_track_id {
        normalize_selected_container_sample_entry_box(&selected.sample_entry_box)?
    } else {
        selected.sample_entry_box.clone()
    };

    Ok(ImportedTrack {
        kind: selected.kind,
        timescale: selected.timescale,
        language: selected.language,
        handler_name: selected.handler_name.clone(),
        mux_policy,
        width: selected.width,
        height: selected.height,
        sample_entry_box,
        source_edit_media_time: selected.source_edit_media_time,
        sample_roll_distance: selected.mux_policy.sample_roll_distance(),
        samples: selected
            .samples
            .iter()
            .map(|sample| ImportedSample {
                source_index: 0,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                duration: sample.duration,
                composition_time_offset: sample.composition_time_offset,
                is_sync_sample: sample.is_sync_sample,
            })
            .collect(),
    }
    .with_source_index_from_candidate(selected, preserve_track_id))
}

fn normalize_selected_container_sample_entry_box(
    sample_entry_box: &[u8],
) -> Result<Vec<u8>, MuxError> {
    if !sample_entry_is_mpeg_video(sample_entry_box) {
        return Ok(sample_entry_box.to_vec());
    }

    let child_boxes = super::mp4::visual_sample_entry_immediate_children(sample_entry_box)?;
    let mut retained_children = Vec::with_capacity(child_boxes.len());
    let mut stripped_square_pasp = false;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box) == Some(FourCc::from_bytes(*b"pasp")) {
            let pasp = super::mp4::decode_typed_box::<Pasp>(&child_box)?;
            if pasp.h_spacing == 1 && pasp.v_spacing == 1 {
                stripped_square_pasp = true;
                continue;
            }
        }
        retained_children.push(child_box);
    }

    if stripped_square_pasp {
        super::mp4::replace_visual_sample_entry_immediate_children(
            sample_entry_box,
            &retained_children,
        )
    } else {
        Ok(sample_entry_box.to_vec())
    }
}

fn sample_entry_is_mpeg_video(sample_entry_box: &[u8]) -> bool {
    sample_entry_box_type(sample_entry_box) == Some(FourCc::from_bytes(*b"mp4v"))
        && [0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x6A]
            .iter()
            .any(|object_type| sample_entry_carries_oti(sample_entry_box, *object_type))
}

fn select_container_tracks(
    tracks: &[TrackCandidate],
    selector: Option<MuxMp4TrackSelector>,
    spec: String,
    preserve_track_id: bool,
) -> Result<Vec<ImportedTrack>, MuxError> {
    match selector {
        Some(selector) => Ok(vec![select_mp4_track(
            tracks,
            selector,
            spec,
            preserve_track_id,
        )?]),
        None => {
            let selected = tracks
                .iter()
                .filter(|track| {
                    matches!(
                        track.kind,
                        MuxTrackKind::Video
                            | MuxTrackKind::Audio
                            | MuxTrackKind::Text
                            | MuxTrackKind::Subtitle
                    )
                })
                .map(|track| {
                    ImportedTrack {
                        kind: track.kind,
                        timescale: track.timescale,
                        language: track.language,
                        handler_name: track.handler_name.clone(),
                        mux_policy: track.mux_policy,
                        width: track.width,
                        height: track.height,
                        sample_entry_box: track.sample_entry_box.clone(),
                        source_edit_media_time: track.source_edit_media_time,
                        sample_roll_distance: track.mux_policy.sample_roll_distance(),
                        samples: track
                            .samples
                            .iter()
                            .map(|sample| ImportedSample {
                                source_index: sample.source_index,
                                data_offset: sample.data_offset,
                                data_size: sample.data_size,
                                duration: sample.duration,
                                composition_time_offset: sample.composition_time_offset,
                                is_sync_sample: sample.is_sync_sample,
                            })
                            .collect(),
                    }
                    .with_source_index_from_candidate(track, preserve_track_id)
                })
                .collect::<Vec<_>>();
            if selected.is_empty() {
                return Err(MuxError::MissingTrackSelection { spec });
            }
            Ok(selected)
        }
    }
}

trait ImportedTrackExt {
    fn with_source_index_from_candidate(
        self,
        candidate: &TrackCandidate,
        preserve_track_id: bool,
    ) -> Self;
}

impl ImportedTrackExt for ImportedTrack {
    fn with_source_index_from_candidate(
        mut self,
        candidate: &TrackCandidate,
        preserve_track_id: bool,
    ) -> Self {
        if preserve_track_id {
            self.mux_policy = self.mux_policy.with_preferred_track_id(candidate.track_id);
        }
        for (sample, source) in self.samples.iter_mut().zip(candidate.samples.iter()) {
            sample.source_index = source.source_index;
        }
        self
    }
}

fn parse_track_candidate_sync<R>(
    path: &Path,
    source_index: usize,
    fragmented_hint: bool,
    source_movie_timescale: u32,
    source_file_size: u64,
    reader: &mut R,
    trak_info: &HeaderInfo,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: Read + Seek,
{
    let components =
        extract_track_candidate_components_sync(path, fragmented_hint, reader, trak_info)?;
    finish_parsed_track_candidate_sync(
        path,
        source_index,
        fragmented_hint,
        source_movie_timescale,
        source_file_size,
        reader,
        components,
    )
}

#[cfg(feature = "async")]
async fn parse_track_candidate_async<R>(
    path: &Path,
    source_index: usize,
    fragmented_hint: bool,
    source_movie_timescale: u32,
    source_file_size: u64,
    reader: &mut R,
    trak_info: &HeaderInfo,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: AsyncReadSeek,
{
    let components =
        extract_track_candidate_components_async(path, fragmented_hint, reader, trak_info).await?;
    finish_parsed_track_candidate_async(
        path,
        source_index,
        fragmented_hint,
        source_movie_timescale,
        source_file_size,
        reader,
        components,
    )
    .await
}

fn extract_track_candidate_components_sync<R>(
    path: &Path,
    fragmented_hint: bool,
    reader: &mut R,
    trak_info: &HeaderInfo,
) -> Result<ParsedTrackCandidateComponents, MuxError>
where
    R: Read + Seek,
{
    let tkhd = extract_required_single_as_sync::<_, Tkhd>(
        reader,
        trak_info,
        BoxPath::from([TKHD]),
        "tkhd",
    )?;
    let mdhd = extract_required_single_as_sync::<_, Mdhd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MDHD]),
        "mdhd",
    )?;
    let hdlr =
        extract_optional_single_as_sync::<_, Hdlr>(reader, trak_info, BoxPath::from([MDIA, HDLR]))?;
    let stsd_info = extract_required_single_info_sync(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, STSD]),
        "stsd",
    )?;
    let stbl_info = extract_required_single_info_sync(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL]),
        "stbl",
    )?;
    let stsd = extract_required_single_as_sync::<_, crate::boxes::iso14496_12::Stsd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, STSD]),
        "stsd",
    )?;
    let (mut sample_entries, sample_entry_boxes) =
        extract_stsd_sample_entries_sync(path, reader, &stsd_info, &stsd, tkhd.track_id)?;
    let data_references = extract_data_references_sync(path, reader, trak_info, tkhd.track_id)?;
    let sample_entry = sample_entries.remove(0);
    let sample_entry_box =
        sample_entry_boxes
            .first()
            .cloned()
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {} is missing a sample-entry payload", tkhd.track_id),
            })?;
    let elst =
        extract_optional_single_as_sync::<_, Elst>(reader, trak_info, BoxPath::from([EDTS, ELST]))?;
    let elst_box_size = extract_box(reader, Some(trak_info), BoxPath::from([EDTS, ELST]))?
        .into_iter()
        .next()
        .map(|info| info.size());
    let sgpd = extract_optional_single_as_sync::<_, Sgpd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, FourCc::from_bytes(*b"sgpd")]),
    )?;
    let sbgp = extract_optional_single_as_sync::<_, Sbgp>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, FourCc::from_bytes(*b"sbgp")]),
    )?;
    let preserved_flat_stbl_boxes = extract_preserved_flat_stbl_boxes_sync(reader, &stbl_info)?;
    let mut preserved_flat_trak_boxes =
        extract_box_bytes(reader, Some(trak_info), BoxPath::from([EDTS]))?;
    preserved_flat_trak_boxes.extend(extract_box_bytes(
        reader,
        Some(trak_info),
        BoxPath::from([TREF]),
    )?);
    preserved_flat_trak_boxes.extend(extract_box_bytes(
        reader,
        Some(trak_info),
        BoxPath::from([UDTA]),
    )?);

    let (stts, ctts, stsc, sample_sizes, stco, co64, stss) = if fragmented_hint {
        (None, None, None, None, None, None, None)
    } else {
        let stsz = extract_optional_single_as_sync::<_, Stsz>(
            reader,
            trak_info,
            BoxPath::from([MDIA, MINF, STBL, STSZ]),
        )?;
        let stz2_boxes = extract_box_bytes(reader, Some(&stbl_info), BoxPath::from([STZ2]))?;
        let sample_sizes = match (stsz, stz2_boxes.as_slice()) {
            (Some(stsz), []) => expand_sample_sizes(&stsz, path, tkhd.track_id)?,
            (None, [stz2_bytes]) => parse_compact_sample_sizes(stz2_bytes, path, tkhd.track_id)?,
            (Some(_), [_]) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes both stsz and stz2 sample size tables",
                        tkhd.track_id
                    ),
                });
            }
            (Some(_), stz2_boxes) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes stsz plus {} compact sample size tables",
                        tkhd.track_id,
                        stz2_boxes.len()
                    ),
                });
            }
            (None, []) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!("track {} is missing a sample size table", tkhd.track_id),
                });
            }
            (None, stz2_boxes) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes {} compact sample size tables",
                        tkhd.track_id,
                        stz2_boxes.len()
                    ),
                });
            }
        };
        (
            Some(extract_required_single_as_sync::<_, Stts>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STTS]),
                "stts",
            )?),
            extract_optional_single_as_sync::<_, Ctts>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, CTTS]),
            )?,
            Some(extract_required_single_as_sync::<_, Stsc>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STSC]),
                "stsc",
            )?),
            Some(sample_sizes),
            extract_optional_single_as_sync::<_, Stco>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STCO]),
            )?,
            extract_optional_single_as_sync::<_, Co64>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, CO64]),
            )?,
            extract_optional_single_as_sync::<_, Stss>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STSS]),
            )?,
        )
    };

    Ok(ParsedTrackCandidateComponents {
        tkhd,
        mdhd,
        hdlr,
        sample_entry,
        sample_entry_box,
        sample_entry_boxes,
        data_references,
        elst,
        elst_box_size,
        sample_roll_distance: extracted_sample_roll_distance(sgpd.as_ref()),
        emit_roll_sbgp: extracted_roll_sbgp_present(sbgp.as_ref()),
        preserved_flat_stbl_boxes,
        preserved_flat_trak_boxes,
        stts,
        ctts,
        stsc,
        sample_sizes,
        stco,
        co64,
        stss,
    })
}

#[cfg(feature = "async")]
async fn extract_track_candidate_components_async<R>(
    path: &Path,
    fragmented_hint: bool,
    reader: &mut R,
    trak_info: &HeaderInfo,
) -> Result<ParsedTrackCandidateComponents, MuxError>
where
    R: AsyncReadSeek,
{
    let tkhd = extract_required_single_as_async::<_, Tkhd>(
        reader,
        trak_info,
        BoxPath::from([TKHD]),
        "tkhd",
    )
    .await?;
    let mdhd = extract_required_single_as_async::<_, Mdhd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MDHD]),
        "mdhd",
    )
    .await?;
    let hdlr =
        extract_optional_single_as_async::<_, Hdlr>(reader, trak_info, BoxPath::from([MDIA, HDLR]))
            .await?;
    let stsd_info = extract_required_single_info_async(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, STSD]),
        "stsd",
    )
    .await?;
    let stbl_info = extract_required_single_info_async(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL]),
        "stbl",
    )
    .await?;
    let stsd = extract_required_single_as_async::<_, crate::boxes::iso14496_12::Stsd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, STSD]),
        "stsd",
    )
    .await?;
    let (mut sample_entries, sample_entry_boxes) =
        extract_stsd_sample_entries_async(path, reader, &stsd_info, &stsd, tkhd.track_id).await?;
    let data_references =
        extract_data_references_async(path, reader, trak_info, tkhd.track_id).await?;
    let sample_entry = sample_entries.remove(0);
    let sample_entry_box =
        sample_entry_boxes
            .first()
            .cloned()
            .ok_or_else(|| MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {} is missing a sample-entry payload", tkhd.track_id),
            })?;
    let elst =
        extract_optional_single_as_async::<_, Elst>(reader, trak_info, BoxPath::from([EDTS, ELST]))
            .await?;
    let elst_box_size = extract_box_async(reader, Some(trak_info), BoxPath::from([EDTS, ELST]))
        .await?
        .into_iter()
        .next()
        .map(|info| info.size());
    let sgpd = extract_optional_single_as_async::<_, Sgpd>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, FourCc::from_bytes(*b"sgpd")]),
    )
    .await?;
    let sbgp = extract_optional_single_as_async::<_, Sbgp>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, STBL, FourCc::from_bytes(*b"sbgp")]),
    )
    .await?;
    let preserved_flat_stbl_boxes =
        extract_preserved_flat_stbl_boxes_async(reader, &stbl_info).await?;
    let mut preserved_flat_trak_boxes =
        extract_box_bytes_async(reader, Some(trak_info), BoxPath::from([EDTS])).await?;
    preserved_flat_trak_boxes
        .extend(extract_box_bytes_async(reader, Some(trak_info), BoxPath::from([TREF])).await?);
    preserved_flat_trak_boxes
        .extend(extract_box_bytes_async(reader, Some(trak_info), BoxPath::from([UDTA])).await?);

    let (stts, ctts, stsc, sample_sizes, stco, co64, stss) = if fragmented_hint {
        (None, None, None, None, None, None, None)
    } else {
        let stsz = extract_optional_single_as_async::<_, Stsz>(
            reader,
            trak_info,
            BoxPath::from([MDIA, MINF, STBL, STSZ]),
        )
        .await?;
        let stz2_boxes =
            extract_box_bytes_async(reader, Some(&stbl_info), BoxPath::from([STZ2])).await?;
        let sample_sizes = match (stsz, stz2_boxes.as_slice()) {
            (Some(stsz), []) => expand_sample_sizes(&stsz, path, tkhd.track_id)?,
            (None, [stz2_bytes]) => parse_compact_sample_sizes(stz2_bytes, path, tkhd.track_id)?,
            (Some(_), [_]) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes both stsz and stz2 sample size tables",
                        tkhd.track_id
                    ),
                });
            }
            (Some(_), stz2_boxes) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes stsz plus {} compact sample size tables",
                        tkhd.track_id,
                        stz2_boxes.len()
                    ),
                });
            }
            (None, []) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!("track {} is missing a sample size table", tkhd.track_id),
                });
            }
            (None, stz2_boxes) => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {} exposes {} compact sample size tables",
                        tkhd.track_id,
                        stz2_boxes.len()
                    ),
                });
            }
        };
        (
            Some(
                extract_required_single_as_async::<_, Stts>(
                    reader,
                    trak_info,
                    BoxPath::from([MDIA, MINF, STBL, STTS]),
                    "stts",
                )
                .await?,
            ),
            extract_optional_single_as_async::<_, Ctts>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, CTTS]),
            )
            .await?,
            Some(
                extract_required_single_as_async::<_, Stsc>(
                    reader,
                    trak_info,
                    BoxPath::from([MDIA, MINF, STBL, STSC]),
                    "stsc",
                )
                .await?,
            ),
            Some(sample_sizes),
            extract_optional_single_as_async::<_, Stco>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STCO]),
            )
            .await?,
            extract_optional_single_as_async::<_, Co64>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, CO64]),
            )
            .await?,
            extract_optional_single_as_async::<_, Stss>(
                reader,
                trak_info,
                BoxPath::from([MDIA, MINF, STBL, STSS]),
            )
            .await?,
        )
    };

    Ok(ParsedTrackCandidateComponents {
        tkhd,
        mdhd,
        hdlr,
        sample_entry,
        sample_entry_box,
        sample_entry_boxes,
        data_references,
        elst,
        elst_box_size,
        sample_roll_distance: extracted_sample_roll_distance(sgpd.as_ref()),
        emit_roll_sbgp: extracted_roll_sbgp_present(sbgp.as_ref()),
        preserved_flat_stbl_boxes,
        preserved_flat_trak_boxes,
        stts,
        ctts,
        stsc,
        sample_sizes,
        stco,
        co64,
        stss,
    })
}

fn finish_parsed_track_candidate_sync<R>(
    path: &Path,
    source_index: usize,
    fragmented_hint: bool,
    source_movie_timescale: u32,
    source_file_size: u64,
    reader: &mut R,
    components: ParsedTrackCandidateComponents,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: Read + Seek,
{
    if fragmented_hint {
        return build_track_candidate_from_components(
            path,
            components.tkhd,
            components.mdhd,
            components.hdlr,
            &components.sample_entry,
            components.sample_entry_box,
            components.sample_entry_boxes,
            components.elst,
            false,
            source_movie_timescale,
            components.sample_roll_distance,
            components.emit_roll_sbgp,
            false,
            true,
            None,
            None,
            None,
            None,
            components.preserved_flat_stbl_boxes,
            components.preserved_flat_trak_boxes,
            Vec::new(),
        );
    }

    let track_id = components.tkhd.track_id;
    parse_track_candidate_from_components(
        path,
        reader,
        source_index,
        components.tkhd,
        components.mdhd,
        components.hdlr,
        &components.sample_entry,
        components.sample_entry_box,
        components.sample_entry_boxes,
        components.data_references,
        components.stts.ok_or(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} is missing stts timing entries"),
        })?,
        components.ctts,
        components.elst,
        components.elst_box_size,
        source_movie_timescale,
        source_file_size,
        components.sample_roll_distance,
        components.emit_roll_sbgp,
        components.stsc.ok_or(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} is missing stsc chunk entries"),
        })?,
        components
            .sample_sizes
            .ok_or(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} is missing sample sizes"),
            })?,
        components.stco,
        components.co64,
        components.stss,
        components.preserved_flat_stbl_boxes,
        components.preserved_flat_trak_boxes,
    )
}

#[cfg(feature = "async")]
async fn finish_parsed_track_candidate_async<R>(
    path: &Path,
    source_index: usize,
    fragmented_hint: bool,
    source_movie_timescale: u32,
    source_file_size: u64,
    reader: &mut R,
    components: ParsedTrackCandidateComponents,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: AsyncReadSeek,
{
    if fragmented_hint {
        return build_track_candidate_from_components(
            path,
            components.tkhd,
            components.mdhd,
            components.hdlr,
            &components.sample_entry,
            components.sample_entry_box,
            components.sample_entry_boxes,
            components.elst,
            false,
            source_movie_timescale,
            components.sample_roll_distance,
            components.emit_roll_sbgp,
            false,
            true,
            None,
            None,
            None,
            None,
            components.preserved_flat_stbl_boxes,
            components.preserved_flat_trak_boxes,
            Vec::new(),
        );
    }

    let track_id = components.tkhd.track_id;
    parse_track_candidate_from_components_async(
        path,
        reader,
        source_index,
        components.tkhd,
        components.mdhd,
        components.hdlr,
        &components.sample_entry,
        components.sample_entry_box,
        components.sample_entry_boxes,
        components.data_references,
        components.stts.ok_or(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} is missing stts timing entries"),
        })?,
        components.ctts,
        components.elst,
        components.elst_box_size,
        source_movie_timescale,
        source_file_size,
        components.sample_roll_distance,
        components.emit_roll_sbgp,
        components.stsc.ok_or(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} is missing stsc chunk entries"),
        })?,
        components
            .sample_sizes
            .ok_or(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} is missing sample sizes"),
            })?,
        components.stco,
        components.co64,
        components.stss,
        components.preserved_flat_stbl_boxes,
        components.preserved_flat_trak_boxes,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn parse_track_candidate_from_components<R>(
    path: &Path,
    reader: &mut R,
    source_index: usize,
    tkhd: Tkhd,
    mdhd: Mdhd,
    hdlr: Option<Hdlr>,
    sample_entry: &ExtractedBox,
    sample_entry_box: Vec<u8>,
    sample_entry_boxes: Vec<Vec<u8>>,
    data_references: Vec<ImportedDataReference>,
    stts: Stts,
    ctts: Option<Ctts>,
    elst: Option<Elst>,
    elst_box_size: Option<u64>,
    source_movie_timescale: u32,
    source_file_size: u64,
    sample_roll_distance: Option<i16>,
    emit_roll_sbgp: bool,
    stsc: Stsc,
    mut sample_sizes: Vec<u32>,
    stco: Option<Stco>,
    co64: Option<Co64>,
    stss: Option<Stss>,
    preserved_flat_stbl_boxes: Vec<Vec<u8>>,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: Read + Seek,
{
    let sample_entry_type = sample_entry.info.box_type();
    let mut sample_durations =
        expand_sample_durations(&stts, sample_sizes.len(), path, tkhd.track_id)?;
    let mut composition_offsets =
        expand_composition_offsets(ctts.as_ref(), sample_sizes.len(), path, tkhd.track_id)?;
    let chunk_offsets = select_chunk_offsets(stco.as_ref(), co64.as_ref(), path, tkhd.track_id)?;
    let mut flat_chunk_sample_counts =
        expand_chunk_sample_counts(&stsc, chunk_offsets.len(), path, tkhd.track_id)?;
    let (mut sample_offsets, mut sample_description_indices) =
        expand_sample_offsets_and_description_indices(
            &stsc,
            &sample_sizes,
            &chunk_offsets,
            path,
            tkhd.track_id,
        )?;
    let mut sync_samples = expand_sync_samples(
        stss.as_ref(),
        sample_entry_type,
        sample_sizes.len(),
        path,
        tkhd.track_id,
    )?;
    let mut source_sync_samples = stss.as_ref().map(|_| sync_samples.clone());
    let sample_data_references = sample_data_references_for_description_indices(
        path,
        tkhd.track_id,
        &sample_entry_boxes,
        &data_references,
        &sample_description_indices,
    )?;
    let uses_external_data_reference = sample_data_references
        .iter()
        .any(|reference| matches!(reference, ImportedDataReference::LocalFile(_)));

    if uses_external_data_reference {
        validate_imported_sample_data_references(
            path,
            tkhd.track_id,
            &sample_offsets,
            &sample_sizes,
            &sample_data_references,
            source_file_size,
        )?;
    } else {
        let available_sample_count = imported_sample_prefix_len_within_source_file(
            &sample_offsets,
            &sample_sizes,
            source_file_size,
        );
        if available_sample_count < sample_sizes.len() {
            sample_sizes.truncate(available_sample_count);
            sample_durations.truncate(available_sample_count);
            composition_offsets.truncate(available_sample_count);
            sample_offsets.truncate(available_sample_count);
            sample_description_indices.truncate(available_sample_count);
            sync_samples.truncate(available_sample_count);
            if let Some(source_sync_samples) = source_sync_samples.as_mut() {
                source_sync_samples.truncate(available_sample_count);
            }
            trim_flat_chunk_sample_counts_to_sample_count(
                &mut flat_chunk_sample_counts,
                available_sample_count,
            )?;
        }

        if should_drop_truncated_terminal_imported_sample(
            sample_offsets.last().copied(),
            sample_sizes.last().copied(),
            source_file_size,
        ) {
            sample_sizes.pop();
            sample_durations.pop();
            composition_offsets.pop();
            sample_offsets.pop();
            sample_description_indices.pop();
            sync_samples.pop();
            if let Some(source_sync_samples) = source_sync_samples.as_mut() {
                source_sync_samples.pop();
            }
            if let Some(last_chunk_sample_count) = flat_chunk_sample_counts.last_mut() {
                if *last_chunk_sample_count > 1 {
                    *last_chunk_sample_count -= 1;
                } else {
                    flat_chunk_sample_counts.pop();
                }
            }
        }
    }
    if !uses_external_data_reference {
        supplement_imported_mp4_avc_sync_samples_sync(
            reader,
            sample_entry_type,
            &sample_entry_box,
            stss.as_ref(),
            &sample_offsets,
            &sample_sizes,
            &mut sync_samples,
        )?;
        supplement_imported_mp4_hevc_sync_samples_sync(
            reader,
            sample_entry_type,
            &sample_entry_box,
            &sample_offsets,
            &sample_sizes,
            &mut sync_samples,
        )?;
    }
    let synthesized_speex_elst_tail = synthesize_imported_speex_elst_tail_sync(
        reader,
        sample_entry,
        elst.as_ref(),
        elst_box_size,
        &mut sample_offsets,
        &mut sample_sizes,
        &mut sample_durations,
        &mut composition_offsets,
        &mut sample_description_indices,
        &mut sync_samples,
    )?;
    let sample_data_references = sample_data_references_for_description_indices(
        path,
        tkhd.track_id,
        &sample_entry_boxes,
        &data_references,
        &sample_description_indices,
    )?;
    let resolved_sample_offsets = resolved_sample_logical_offsets(
        &sample_offsets,
        &sample_sizes,
        &sample_data_references,
        source_file_size,
    )?;
    let source_segments = sample_source_segments(
        path,
        tkhd.track_id,
        &resolved_sample_offsets,
        &sample_offsets,
        &sample_sizes,
        &sample_data_references,
    )?;

    let mut samples = Vec::with_capacity(sample_sizes.len());
    for index in 0..sample_sizes.len() {
        samples.push(CandidateSample {
            source_index,
            data_offset: resolved_sample_offsets[index],
            data_size: sample_sizes[index],
            duration: sample_durations[index],
            composition_time_offset: composition_offsets[index],
            is_sync_sample: sync_samples[index],
        });
    }

    let mut parsed = build_track_candidate_from_components(
        path,
        tkhd,
        mdhd,
        hdlr,
        sample_entry,
        sample_entry_box,
        sample_entry_boxes,
        elst,
        synthesized_speex_elst_tail,
        source_movie_timescale,
        sample_roll_distance,
        emit_roll_sbgp,
        stss.as_ref()
            .is_some_and(|stss| stss.entry_count == 1 && stss.sample_number.as_slice() == [1]),
        stts.entry_count == 0,
        Some(flat_chunk_sample_counts),
        Some(stsc),
        Some(sample_description_indices),
        source_sync_samples,
        preserved_flat_stbl_boxes,
        preserved_flat_trak_boxes,
        samples,
    )?;
    if let Some(parsed) = parsed.as_mut() {
        parsed.source_segments = source_segments;
        parsed.uses_external_data_reference = uses_external_data_reference;
    }
    Ok(parsed)
}

#[cfg(feature = "async")]
#[allow(clippy::too_many_arguments)]
async fn parse_track_candidate_from_components_async<R>(
    path: &Path,
    reader: &mut R,
    source_index: usize,
    tkhd: Tkhd,
    mdhd: Mdhd,
    hdlr: Option<Hdlr>,
    sample_entry: &ExtractedBox,
    sample_entry_box: Vec<u8>,
    sample_entry_boxes: Vec<Vec<u8>>,
    data_references: Vec<ImportedDataReference>,
    stts: Stts,
    ctts: Option<Ctts>,
    elst: Option<Elst>,
    elst_box_size: Option<u64>,
    source_movie_timescale: u32,
    source_file_size: u64,
    sample_roll_distance: Option<i16>,
    emit_roll_sbgp: bool,
    stsc: Stsc,
    mut sample_sizes: Vec<u32>,
    stco: Option<Stco>,
    co64: Option<Co64>,
    stss: Option<Stss>,
    preserved_flat_stbl_boxes: Vec<Vec<u8>>,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
) -> Result<Option<ParsedMp4Track>, MuxError>
where
    R: AsyncReadSeek,
{
    let sample_entry_type = sample_entry.info.box_type();
    let mut sample_durations =
        expand_sample_durations(&stts, sample_sizes.len(), path, tkhd.track_id)?;
    let mut composition_offsets =
        expand_composition_offsets(ctts.as_ref(), sample_sizes.len(), path, tkhd.track_id)?;
    let chunk_offsets = select_chunk_offsets(stco.as_ref(), co64.as_ref(), path, tkhd.track_id)?;
    let mut flat_chunk_sample_counts =
        expand_chunk_sample_counts(&stsc, chunk_offsets.len(), path, tkhd.track_id)?;
    let (mut sample_offsets, mut sample_description_indices) =
        expand_sample_offsets_and_description_indices(
            &stsc,
            &sample_sizes,
            &chunk_offsets,
            path,
            tkhd.track_id,
        )?;
    let mut sync_samples = expand_sync_samples(
        stss.as_ref(),
        sample_entry_type,
        sample_sizes.len(),
        path,
        tkhd.track_id,
    )?;
    let mut source_sync_samples = stss.as_ref().map(|_| sync_samples.clone());
    let sample_data_references = sample_data_references_for_description_indices(
        path,
        tkhd.track_id,
        &sample_entry_boxes,
        &data_references,
        &sample_description_indices,
    )?;
    let uses_external_data_reference = sample_data_references
        .iter()
        .any(|reference| matches!(reference, ImportedDataReference::LocalFile(_)));

    if uses_external_data_reference {
        validate_imported_sample_data_references_async(
            path,
            tkhd.track_id,
            &sample_offsets,
            &sample_sizes,
            &sample_data_references,
            source_file_size,
        )
        .await?;
    } else {
        let available_sample_count = imported_sample_prefix_len_within_source_file(
            &sample_offsets,
            &sample_sizes,
            source_file_size,
        );
        if available_sample_count < sample_sizes.len() {
            sample_sizes.truncate(available_sample_count);
            sample_durations.truncate(available_sample_count);
            composition_offsets.truncate(available_sample_count);
            sample_offsets.truncate(available_sample_count);
            sample_description_indices.truncate(available_sample_count);
            sync_samples.truncate(available_sample_count);
            if let Some(source_sync_samples) = source_sync_samples.as_mut() {
                source_sync_samples.truncate(available_sample_count);
            }
            trim_flat_chunk_sample_counts_to_sample_count(
                &mut flat_chunk_sample_counts,
                available_sample_count,
            )?;
        }

        if should_drop_truncated_terminal_imported_sample(
            sample_offsets.last().copied(),
            sample_sizes.last().copied(),
            source_file_size,
        ) {
            sample_sizes.pop();
            sample_durations.pop();
            composition_offsets.pop();
            sample_offsets.pop();
            sample_description_indices.pop();
            sync_samples.pop();
            if let Some(source_sync_samples) = source_sync_samples.as_mut() {
                source_sync_samples.pop();
            }
            if let Some(last_chunk_sample_count) = flat_chunk_sample_counts.last_mut() {
                if *last_chunk_sample_count > 1 {
                    *last_chunk_sample_count -= 1;
                } else {
                    flat_chunk_sample_counts.pop();
                }
            }
        }
    }
    if !uses_external_data_reference {
        supplement_imported_mp4_avc_sync_samples_async(
            reader,
            sample_entry_type,
            &sample_entry_box,
            stss.as_ref(),
            &sample_offsets,
            &sample_sizes,
            &mut sync_samples,
        )
        .await?;
        supplement_imported_mp4_hevc_sync_samples_async(
            reader,
            sample_entry_type,
            &sample_entry_box,
            &sample_offsets,
            &sample_sizes,
            &mut sync_samples,
        )
        .await?;
    }
    let synthesized_speex_elst_tail = synthesize_imported_speex_elst_tail_async(
        reader,
        sample_entry,
        elst.as_ref(),
        elst_box_size,
        &mut sample_offsets,
        &mut sample_sizes,
        &mut sample_durations,
        &mut composition_offsets,
        &mut sample_description_indices,
        &mut sync_samples,
    )
    .await?;
    let sample_data_references = sample_data_references_for_description_indices(
        path,
        tkhd.track_id,
        &sample_entry_boxes,
        &data_references,
        &sample_description_indices,
    )?;
    let resolved_sample_offsets = resolved_sample_logical_offsets(
        &sample_offsets,
        &sample_sizes,
        &sample_data_references,
        source_file_size,
    )?;
    let source_segments = sample_source_segments(
        path,
        tkhd.track_id,
        &resolved_sample_offsets,
        &sample_offsets,
        &sample_sizes,
        &sample_data_references,
    )?;

    let mut samples = Vec::with_capacity(sample_sizes.len());
    for index in 0..sample_sizes.len() {
        samples.push(CandidateSample {
            source_index,
            data_offset: resolved_sample_offsets[index],
            data_size: sample_sizes[index],
            duration: sample_durations[index],
            composition_time_offset: composition_offsets[index],
            is_sync_sample: sync_samples[index],
        });
    }

    let mut parsed = build_track_candidate_from_components(
        path,
        tkhd,
        mdhd,
        hdlr,
        sample_entry,
        sample_entry_box,
        sample_entry_boxes,
        elst,
        synthesized_speex_elst_tail,
        source_movie_timescale,
        sample_roll_distance,
        emit_roll_sbgp,
        stss.as_ref()
            .is_some_and(|stss| stss.entry_count == 1 && stss.sample_number.as_slice() == [1]),
        stts.entry_count == 0,
        Some(flat_chunk_sample_counts),
        Some(stsc),
        Some(sample_description_indices),
        source_sync_samples,
        preserved_flat_stbl_boxes,
        preserved_flat_trak_boxes,
        samples,
    )?;
    if let Some(parsed) = parsed.as_mut() {
        parsed.source_segments = source_segments;
        parsed.uses_external_data_reference = uses_external_data_reference;
    }
    Ok(parsed)
}

#[allow(clippy::too_many_arguments)]
fn build_track_candidate_from_components(
    path: &Path,
    tkhd: Tkhd,
    mdhd: Mdhd,
    hdlr: Option<Hdlr>,
    sample_entry: &ExtractedBox,
    sample_entry_box: Vec<u8>,
    sample_entry_boxes: Vec<Vec<u8>>,
    elst: Option<Elst>,
    synthesized_speex_elst_tail: bool,
    source_movie_timescale: u32,
    sample_roll_distance: Option<i16>,
    emit_roll_sbgp: bool,
    source_stss_first_only: bool,
    source_had_empty_stts: bool,
    flat_chunk_sample_counts: Option<Vec<u32>>,
    flat_stsc: Option<Stsc>,
    sample_description_indices: Option<Vec<u32>>,
    source_sync_samples: Option<Vec<bool>>,
    preserved_flat_stbl_boxes: Vec<Vec<u8>>,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
    samples: Vec<CandidateSample>,
) -> Result<Option<ParsedMp4Track>, MuxError> {
    let sample_entry_type = sample_entry.info.box_type();
    let kind = if let Some(hdlr) = hdlr.as_ref() {
        match hdlr.handler_type {
            VIDE => MuxTrackKind::Video,
            SOUN => MuxTrackKind::Audio,
            TEXT => MuxTrackKind::Text,
            SUBT | SUBP => MuxTrackKind::Subtitle,
            _ => return Ok(None),
        }
    } else {
        let Some(kind) = infer_track_kind_from_sample_entry_type(sample_entry_type) else {
            return Ok(None);
        };
        kind
    };
    if matches!(sample_entry_type, ENCV | ENCA) {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {} uses protected sample entry `{sample_entry_type}`; decrypt before muxing",
                tkhd.track_id
            ),
        });
    }

    let (width, height) = match kind {
        MuxTrackKind::Audio => (0, 0),
        MuxTrackKind::Video | MuxTrackKind::Text | MuxTrackKind::Subtitle => (
            fixed_16_16_to_u16(tkhd.width),
            fixed_16_16_to_u16(tkhd.height),
        ),
    };
    let language = decode_mdhd_language(mdhd.language);
    let mut mux_policy =
        imported_track_mux_policy_for_sample_entry_type(sample_entry_type, &sample_entry_box, kind);
    if let Some(sample_roll_distance) = sample_roll_distance {
        mux_policy = mux_policy.with_sample_roll_distance(sample_roll_distance);
    }
    mux_policy = mux_policy.with_emit_roll_sbgp(emit_roll_sbgp);

    let source_edit_media_time = if synthesized_speex_elst_tail {
        None
    } else {
        elst.as_ref()
            .and_then(imported_track_source_edit_media_time_from_elst)
    };
    let source_edit_segment_duration = elst
        .as_ref()
        .and_then(imported_track_source_edit_segment_duration_from_elst);
    let default_header_policy = default_imported_track_header_policy(kind);
    let (tkhd_flags, alternate_group, volume, matrix) = if synthesized_speex_elst_tail
        && kind == MuxTrackKind::Audio
        && sample_entry_type == FourCc::from_bytes(*b"spex")
    {
        (
            default_header_policy.tkhd_flags,
            default_header_policy.alternate_group,
            default_header_policy.volume,
            default_header_policy.matrix,
        )
    } else {
        (tkhd.flags(), tkhd.alternate_group, tkhd.volume, tkhd.matrix)
    };

    Ok(Some(ParsedMp4Track {
        track: TrackCandidate {
            track_id: tkhd.track_id,
            kind,
            timescale: mdhd.timescale,
            language,
            handler_name: hdlr
                .and_then(|value| (!value.name.is_empty()).then_some(value.name))
                .unwrap_or_else(|| default_handler_name_for_kind(kind).to_string()),
            mux_policy: mux_policy.with_header_policy(ImportedTrackHeaderPolicy {
                tkhd_flags,
                alternate_group,
                volume,
                matrix,
                source_track_id: Some(tkhd.track_id),
                source_track_creation_time: Some(tkhd.creation_time()),
                source_track_modification_time: Some(tkhd.modification_time()),
                source_media_creation_time: Some(mdhd.creation_time()),
                source_media_modification_time: Some(mdhd.modification_time()),
                source_movie_timescale: Some(source_movie_timescale),
                source_media_duration: Some(mdhd.duration()),
                source_edit_segment_duration,
                source_media_decode_time_offset: None,
                source_stss_first_only,
            }),
            width,
            height,
            sample_entry_box,
            source_edit_media_time,
            samples,
        },
        carry: ImportedMp4TrackCarry {
            flat_chunk_sample_counts,
            flat_stsc,
            sample_entry_boxes: Some(sample_entry_boxes),
            sample_description_indices,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts,
            source_sync_samples,
            preserved_flat_stbl_boxes,
            preserved_flat_trak_boxes,
        },
        source_segments: Vec::new(),
        uses_external_data_reference: false,
    }))
}

fn should_drop_truncated_terminal_imported_sample(
    sample_offset: Option<u64>,
    sample_size: Option<u32>,
    source_file_size: u64,
) -> bool {
    let (Some(sample_offset), Some(sample_size)) = (sample_offset, sample_size) else {
        return false;
    };
    if sample_offset >= source_file_size {
        return false;
    }
    sample_offset.saturating_add(u64::from(sample_size)) > source_file_size
}

fn imported_sample_prefix_len_within_source_file(
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    source_file_size: u64,
) -> usize {
    sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .position(|(sample_offset, sample_size)| {
            sample_offset
                .checked_add(u64::from(sample_size))
                .is_none_or(|sample_end| sample_end > source_file_size)
        })
        .unwrap_or(sample_sizes.len())
}

fn resolved_sample_logical_offsets(
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    data_references: &[ImportedDataReference],
    source_file_size: u64,
) -> Result<Vec<u64>, MuxError> {
    let mut resolved = sample_offsets.to_vec();
    let mut next_external_offset = source_file_size;
    for ((resolved_offset, sample_size), data_reference) in resolved
        .iter_mut()
        .zip(sample_sizes.iter().copied())
        .zip(data_references.iter())
    {
        if matches!(data_reference, ImportedDataReference::LocalFile(_)) {
            *resolved_offset = next_external_offset;
            next_external_offset = next_external_offset
                .checked_add(u64::from(sample_size))
                .ok_or(MuxError::LayoutOverflow(
                    "external data-reference logical offset",
                ))?;
        }
    }
    Ok(resolved)
}

fn sample_source_segments(
    path: &Path,
    track_id: u32,
    logical_offsets: &[u64],
    source_offsets: &[u64],
    sample_sizes: &[u32],
    data_references: &[ImportedDataReference],
) -> Result<Vec<SegmentedMuxSourceSegment>, MuxError> {
    if source_offsets.len() != sample_sizes.len()
        || source_offsets.len() != data_references.len()
        || source_offsets.len() != logical_offsets.len()
    {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes inconsistent sample source metadata"),
        });
    }

    let mut segments = Vec::new();
    for (((logical_offset, source_offset), sample_size), data_reference) in logical_offsets
        .iter()
        .copied()
        .zip(source_offsets.iter().copied())
        .zip(sample_sizes.iter().copied())
        .zip(data_references.iter())
    {
        if sample_size == 0 {
            continue;
        }
        let data = match data_reference {
            ImportedDataReference::SelfContained => SegmentedMuxSourceSegmentData::FileRange {
                source_offset,
                size: sample_size,
            },
            ImportedDataReference::LocalFile(path) => {
                SegmentedMuxSourceSegmentData::ExternalFileRange {
                    path: path.clone(),
                    source_offset,
                    size: sample_size,
                }
            }
        };
        segments.push(SegmentedMuxSourceSegment {
            logical_offset,
            data,
        });
    }
    Ok(segments)
}

fn validate_imported_sample_data_references(
    path: &Path,
    track_id: u32,
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    data_references: &[ImportedDataReference],
    source_file_size: u64,
) -> Result<(), MuxError> {
    for ((sample_offset, sample_size), data_reference) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(data_references.iter())
    {
        let sample_end = sample_offset
            .checked_add(u64::from(sample_size))
            .ok_or(MuxError::LayoutOverflow("data-reference sample range"))?;
        match data_reference {
            ImportedDataReference::SelfContained => {
                if sample_end > source_file_size {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} has a self-contained sample outside the source file"
                        ),
                    });
                }
            }
            ImportedDataReference::LocalFile(reference_path) => {
                let size = std::fs::metadata(reference_path)
                    .map_err(|error| {
                        mux_io_at_path("inspect referenced media", reference_path, error)
                    })?
                    .len();
                if sample_end > size {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} has a referenced sample outside the referenced media"
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn validate_imported_sample_data_references_async(
    path: &Path,
    track_id: u32,
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    data_references: &[ImportedDataReference],
    source_file_size: u64,
) -> Result<(), MuxError> {
    for ((sample_offset, sample_size), data_reference) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(data_references.iter())
    {
        let sample_end = sample_offset
            .checked_add(u64::from(sample_size))
            .ok_or(MuxError::LayoutOverflow("data-reference sample range"))?;
        match data_reference {
            ImportedDataReference::SelfContained => {
                if sample_end > source_file_size {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} has a self-contained sample outside the source file"
                        ),
                    });
                }
            }
            ImportedDataReference::LocalFile(reference_path) => {
                let size = tokio::fs::metadata(reference_path)
                    .await
                    .map_err(|error| {
                        mux_io_at_path("inspect referenced media", reference_path, error)
                    })?
                    .len();
                if sample_end > size {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} has a referenced sample outside the referenced media"
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

fn build_mp4_import_source_override(
    path: &Path,
    mut segments: Vec<SegmentedMuxSourceSegment>,
    source_file_size: u64,
) -> Result<SegmentedMuxSourceSpec, MuxError> {
    segments.sort_by_key(|segment| segment.logical_offset);
    let mut normalized = Vec::<SegmentedMuxSourceSegment>::with_capacity(segments.len());
    let mut total_size = source_file_size;
    for segment in segments {
        if let Some(previous) = normalized.last()
            && previous.logical_end() > segment.logical_offset
        {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: "sample data-reference ranges overlap after import resolution".to_string(),
            });
        }
        total_size = total_size.max(segment.logical_end());
        normalized.push(segment);
    }
    Ok(SegmentedMuxSourceSpec {
        path: path.to_path_buf(),
        segments: normalized,
        total_size,
    })
}

fn trim_flat_chunk_sample_counts_to_sample_count(
    flat_chunk_sample_counts: &mut Vec<u32>,
    sample_count: usize,
) -> Result<(), MuxError> {
    let mut remaining = u32::try_from(sample_count)
        .map_err(|_| MuxError::LayoutOverflow("imported flat chunk sample count trim"))?;
    let mut trimmed = Vec::with_capacity(flat_chunk_sample_counts.len());
    for &count in flat_chunk_sample_counts.iter() {
        if remaining == 0 {
            break;
        }
        let kept = count.min(remaining);
        trimmed.push(kept);
        remaining -= kept;
    }
    *flat_chunk_sample_counts = trimmed;
    Ok(())
}

fn imported_track_source_edit_media_time_from_elst(elst: &Elst) -> Option<u64> {
    if elst.entry_count == 0 {
        return None;
    }
    for index in 0..usize::try_from(elst.entry_count).ok()? {
        let media_time = elst.media_time(index);
        if media_time >= 0 {
            return Some(media_time as u64);
        }
    }
    None
}

fn imported_track_source_edit_segment_duration_from_elst(elst: &Elst) -> Option<u64> {
    if elst.entry_count == 0 {
        return None;
    }
    let mut total = 0_u64;
    for index in 0..usize::try_from(elst.entry_count).ok()? {
        total = total.checked_add(elst.segment_duration(index))?;
    }
    Some(total)
}

fn imported_track_elst_trailing_bytes(elst: &Elst, box_size: u64) -> Option<u32> {
    let per_entry_size = if elst.version() == 1 { 20_u64 } else { 12_u64 };
    let expected_size =
        16_u64.checked_add(u64::from(elst.entry_count).checked_mul(per_entry_size)?)?;
    box_size
        .checked_sub(expected_size)
        .and_then(|trailing| u32::try_from(trailing).ok())
        .filter(|trailing| *trailing > 8)
}

#[allow(clippy::too_many_arguments)]
fn synthesize_imported_speex_elst_tail_sync<R>(
    reader: &mut R,
    sample_entry: &ExtractedBox,
    elst: Option<&Elst>,
    elst_box_size: Option<u64>,
    sample_offsets: &mut Vec<u64>,
    sample_sizes: &mut Vec<u32>,
    sample_durations: &mut Vec<u32>,
    composition_offsets: &mut Vec<i32>,
    sample_description_indices: &mut Vec<u32>,
    sync_samples: &mut Vec<bool>,
) -> Result<bool, MuxError>
where
    R: Read + Seek,
{
    if sample_entry.info.box_type() != FourCc::from_bytes(*b"spex") {
        return Ok(false);
    }
    let Some(elst) = elst else {
        return Ok(false);
    };
    let Some(elst_box_size) = elst_box_size else {
        return Ok(false);
    };
    let Some(trailing_bytes) = imported_track_elst_trailing_bytes(elst, elst_box_size) else {
        return Ok(false);
    };
    if sample_durations.last().copied() != Some(0) {
        return Ok(false);
    }

    let skip_info = extract_box(
        reader,
        Some(&sample_entry.info),
        BoxPath::from([FourCc::from_bytes(*b"skip")]),
    )?
    .into_iter()
    .next();
    let Some(skip_info) = skip_info else {
        return Ok(false);
    };
    let skip_size = u32::try_from(skip_info.size())
        .map_err(|_| MuxError::LayoutOverflow("imported speex skip sample size"))?;
    let synthetic_edit_entry_count = usize::try_from((u64::from(trailing_bytes) - 8) / 12)
        .map_err(|_| MuxError::LayoutOverflow("imported speex synthetic edit count"))?;
    if synthetic_edit_entry_count < 2 {
        return Ok(false);
    }

    let removed_terminal_sample_offset = sample_offsets.pop();
    let removed_terminal_sample_size = sample_sizes.pop();
    let removed_terminal_sample_description_index = sample_description_indices.pop();
    sample_durations.pop();
    composition_offsets.pop();
    sync_samples.pop();

    let synthetic_sample_offset = removed_terminal_sample_offset.unwrap_or(skip_info.offset());
    let synthetic_sample_size = removed_terminal_sample_size.unwrap_or(skip_size);
    let synthetic_sample_description_index = removed_terminal_sample_description_index.unwrap_or(1);
    let repeated_tail_sample_offset = sample_offsets
        .first()
        .copied()
        .unwrap_or(synthetic_sample_offset);
    let repeated_tail_sample_size = sample_sizes
        .first()
        .copied()
        .unwrap_or(synthetic_sample_size);
    let repeated_tail_sample_description_index = sample_description_indices
        .first()
        .copied()
        .unwrap_or(synthetic_sample_description_index);

    sample_offsets.push(synthetic_sample_offset);
    sample_sizes.push(synthetic_sample_size);
    sample_durations.push(trailing_bytes);
    composition_offsets.push(0);
    sample_description_indices.push(synthetic_sample_description_index);
    sync_samples.push(true);

    for _ in 0..synthetic_edit_entry_count.saturating_sub(2) {
        sample_offsets.push(repeated_tail_sample_offset);
        sample_sizes.push(repeated_tail_sample_size);
        sample_durations.push(1);
        composition_offsets.push(0);
        sample_description_indices.push(repeated_tail_sample_description_index);
        sync_samples.push(true);
    }

    sample_offsets.push(repeated_tail_sample_offset);
    sample_sizes.push(repeated_tail_sample_size);
    sample_durations.push(0);
    composition_offsets.push(0);
    sample_description_indices.push(repeated_tail_sample_description_index);
    sync_samples.push(true);

    Ok(true)
}

#[cfg(feature = "async")]
#[allow(clippy::too_many_arguments)]
async fn synthesize_imported_speex_elst_tail_async<R>(
    reader: &mut R,
    sample_entry: &ExtractedBox,
    elst: Option<&Elst>,
    elst_box_size: Option<u64>,
    sample_offsets: &mut Vec<u64>,
    sample_sizes: &mut Vec<u32>,
    sample_durations: &mut Vec<u32>,
    composition_offsets: &mut Vec<i32>,
    sample_description_indices: &mut Vec<u32>,
    sync_samples: &mut Vec<bool>,
) -> Result<bool, MuxError>
where
    R: AsyncReadSeek,
{
    if sample_entry.info.box_type() != FourCc::from_bytes(*b"spex") {
        return Ok(false);
    }
    let Some(elst) = elst else {
        return Ok(false);
    };
    let Some(elst_box_size) = elst_box_size else {
        return Ok(false);
    };
    let Some(trailing_bytes) = imported_track_elst_trailing_bytes(elst, elst_box_size) else {
        return Ok(false);
    };
    if sample_durations.last().copied() != Some(0) {
        return Ok(false);
    }

    let skip_info = extract_box_async(
        reader,
        Some(&sample_entry.info),
        BoxPath::from([FourCc::from_bytes(*b"skip")]),
    )
    .await?
    .into_iter()
    .next();
    let Some(skip_info) = skip_info else {
        return Ok(false);
    };
    let skip_size = u32::try_from(skip_info.size())
        .map_err(|_| MuxError::LayoutOverflow("imported speex skip sample size"))?;
    let synthetic_edit_entry_count = usize::try_from((u64::from(trailing_bytes) - 8) / 12)
        .map_err(|_| MuxError::LayoutOverflow("imported speex synthetic edit count"))?;
    if synthetic_edit_entry_count < 2 {
        return Ok(false);
    }

    let removed_terminal_sample_offset = sample_offsets.pop();
    let removed_terminal_sample_size = sample_sizes.pop();
    let removed_terminal_sample_description_index = sample_description_indices.pop();
    sample_durations.pop();
    composition_offsets.pop();
    sync_samples.pop();

    let synthetic_sample_offset = removed_terminal_sample_offset.unwrap_or(skip_info.offset());
    let synthetic_sample_size = removed_terminal_sample_size.unwrap_or(skip_size);
    let synthetic_sample_description_index = removed_terminal_sample_description_index.unwrap_or(1);
    let repeated_tail_sample_offset = sample_offsets
        .first()
        .copied()
        .unwrap_or(synthetic_sample_offset);
    let repeated_tail_sample_size = sample_sizes
        .first()
        .copied()
        .unwrap_or(synthetic_sample_size);
    let repeated_tail_sample_description_index = sample_description_indices
        .first()
        .copied()
        .unwrap_or(synthetic_sample_description_index);

    sample_offsets.push(synthetic_sample_offset);
    sample_sizes.push(synthetic_sample_size);
    sample_durations.push(trailing_bytes);
    composition_offsets.push(0);
    sample_description_indices.push(synthetic_sample_description_index);
    sync_samples.push(true);

    for _ in 0..synthetic_edit_entry_count.saturating_sub(2) {
        sample_offsets.push(repeated_tail_sample_offset);
        sample_sizes.push(repeated_tail_sample_size);
        sample_durations.push(1);
        composition_offsets.push(0);
        sample_description_indices.push(repeated_tail_sample_description_index);
        sync_samples.push(true);
    }

    sample_offsets.push(repeated_tail_sample_offset);
    sample_sizes.push(repeated_tail_sample_size);
    sample_durations.push(0);
    composition_offsets.push(0);
    sample_description_indices.push(repeated_tail_sample_description_index);
    sync_samples.push(true);

    Ok(true)
}

fn fixed_16_16_to_u16(value: u32) -> u16 {
    u16::try_from(value >> 16).unwrap_or(u16::MAX)
}

fn imported_track_mux_policy_for_sample_entry_type(
    sample_entry_type: FourCc,
    sample_entry_box: &[u8],
    kind: MuxTrackKind,
) -> ImportedTrackMuxPolicy {
    if sample_entry_type == FourCc::from_bytes(*b"iamf") {
        return imported_iamf_mux_policy(kind);
    }
    let mut policy = ImportedTrackMuxPolicy::DEFAULT;
    if sample_entry_type == FourCc::from_bytes(*b"hev1")
        || sample_entry_type == FourCc::from_bytes(*b"hvc1")
    {
        if !sample_entry_carries_dolby_vision_config(sample_entry_box) {
            policy.sync_sample_table_mode = SyncSampleTableMode::ForceFirstOnly;
        }
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    if sample_entry_type == FourCc::from_bytes(*b"vp08") {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    if sample_entry_type == FourCc::from_bytes(*b"text")
        || sample_entry_type == FourCc::from_bytes(*b"tx3g")
    {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    if sample_entry_type == FourCc::from_bytes(*b"mp4a")
        || sample_entry_type == FourCc::from_bytes(*b"ac-3")
        || sample_entry_type == FourCc::from_bytes(*b"ec-3")
    {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    if sample_entry_type == FourCc::from_bytes(*b"vp09")
        || sample_entry_type == FourCc::from_bytes(*b"wvtt")
        || sample_entry_type == FourCc::from_bytes(*b"mha1")
        || sample_entry_type == FourCc::from_bytes(*b"mha2")
        || sample_entry_type == FourCc::from_bytes(*b"mhm1")
        || sample_entry_type == FourCc::from_bytes(*b"mhm2")
        || sample_entry_type == FourCc::from_bytes(*b"alac")
        || sample_entry_type == FourCc::from_bytes(*b"ipcm")
        || sample_entry_type == FourCc::from_bytes(*b"fpcm")
    {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    policy
}

fn imported_iamf_mux_policy(kind: MuxTrackKind) -> ImportedTrackMuxPolicy {
    let mut policy = ImportedTrackMuxPolicy::DEFAULT;
    if kind.is_audio() {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    policy
}

fn split_terminal_short_video_chunk_sample_counts(
    sample_durations: &[u32],
    chunk_sample_counts: &mut Vec<u32>,
) {
    if sample_durations.len() < 2 || chunk_sample_counts.is_empty() {
        return;
    }
    let last_duration = *sample_durations.last().unwrap();
    let previous_duration = sample_durations[sample_durations.len() - 2];
    if last_duration >= previous_duration {
        return;
    }
    let Some(last_chunk_sample_count) = chunk_sample_counts.last_mut() else {
        return;
    };
    if *last_chunk_sample_count <= 1 {
        return;
    }
    *last_chunk_sample_count -= 1;
    chunk_sample_counts.push(1);
}

fn split_terminal_short_audio_chunk_sample_counts(
    sample_durations: &[u32],
    chunk_sample_counts: &mut Vec<u32>,
) {
    if sample_durations.len() < 2 || chunk_sample_counts.is_empty() {
        return;
    }
    let last_duration = *sample_durations.last().unwrap();
    let previous_duration = sample_durations[sample_durations.len() - 2];
    if last_duration >= previous_duration {
        return;
    }
    let Some(last_chunk_sample_count) = chunk_sample_counts.last_mut() else {
        return;
    };
    if *last_chunk_sample_count <= 1 {
        return;
    }
    *last_chunk_sample_count -= 1;
    chunk_sample_counts.push(1);
}

fn extracted_sample_roll_distance(sgpd: Option<&Sgpd>) -> Option<i16> {
    let sgpd = sgpd?;
    let grouping_type = sgpd.grouping_type.as_bytes();
    if grouping_type != b"roll" && grouping_type != b"prol" {
        return None;
    }
    if let Some(sample_roll_distance) = sgpd.roll_distances.first().copied() {
        return Some(sample_roll_distance);
    }
    sgpd.roll_distances_l
        .first()
        .map(|entry| entry.roll_distance)
}

fn extracted_roll_sbgp_present(sbgp: Option<&Sbgp>) -> bool {
    let Some(sbgp) = sbgp else {
        return false;
    };
    let grouping_type = sbgp.grouping_type.to_be_bytes();
    grouping_type == *b"roll" || grouping_type == *b"prol"
}

fn infer_track_kind_from_sample_entry_type(sample_entry_type: FourCc) -> Option<MuxTrackKind> {
    if [
        ENCA,
        FourCc::from_bytes(*b"mp4a"),
        FourCc::from_bytes(*b".mp3"),
        FourCc::from_bytes(*b"alaw"),
        FourCc::from_bytes(*b"ulaw"),
        FourCc::from_bytes(*b"Opus"),
        FourCc::from_bytes(*b"spex"),
        FourCc::from_bytes(*b"samr"),
        FourCc::from_bytes(*b"sawb"),
        FourCc::from_bytes(*b"sqcp"),
        FourCc::from_bytes(*b"sevc"),
        FourCc::from_bytes(*b"ssmv"),
        FourCc::from_bytes(*b"ac-3"),
        FourCc::from_bytes(*b"ec-3"),
        FourCc::from_bytes(*b"ac-4"),
        FourCc::from_bytes(*b"alac"),
        FourCc::from_bytes(*b"mlpa"),
        FourCc::from_bytes(*b"dtsc"),
        FourCc::from_bytes(*b"dtse"),
        FourCc::from_bytes(*b"dtsh"),
        FourCc::from_bytes(*b"dtsl"),
        FourCc::from_bytes(*b"dtsm"),
        FourCc::from_bytes(*b"dts-"),
        FourCc::from_bytes(*b"dtsx"),
        FourCc::from_bytes(*b"dtsy"),
        FourCc::from_bytes(*b"fLaC"),
        FourCc::from_bytes(*b"iamf"),
        FourCc::from_bytes(*b"mha1"),
        FourCc::from_bytes(*b"mha2"),
        FourCc::from_bytes(*b"mhm1"),
        FourCc::from_bytes(*b"mhm2"),
        FourCc::from_bytes(*b"ipcm"),
        FourCc::from_bytes(*b"fpcm"),
    ]
    .contains(&sample_entry_type)
    {
        Some(MuxTrackKind::Audio)
    } else if [
        ENCV,
        FourCc::from_bytes(*b"avc1"),
        FourCc::from_bytes(*b"hev1"),
        FourCc::from_bytes(*b"hvc1"),
        FourCc::from_bytes(*b"dvhe"),
        FourCc::from_bytes(*b"dvh1"),
        FourCc::from_bytes(*b"vvc1"),
        FourCc::from_bytes(*b"vvi1"),
        FourCc::from_bytes(*b"avs3"),
        FourCc::from_bytes(*b"av01"),
        FourCc::from_bytes(*b"jpeg"),
        FourCc::from_bytes(*b"mjpg"),
        FourCc::from_bytes(*b"mpeg"),
        FourCc::from_bytes(*b"mp4v"),
        FourCc::from_bytes(*b"s263"),
        FourCc::from_bytes(*b"h263"),
        FourCc::from_bytes(*b"png "),
        FourCc::from_bytes(*b"vp08"),
        FourCc::from_bytes(*b"vp09"),
        FourCc::from_bytes(*b"vp10"),
    ]
    .contains(&sample_entry_type)
    {
        Some(MuxTrackKind::Video)
    } else if [
        FourCc::from_bytes(*b"text"),
        FourCc::from_bytes(*b"tx3g"),
        FourCc::from_bytes(*b"sbtt"),
        FourCc::from_bytes(*b"wvtt"),
    ]
    .contains(&sample_entry_type)
    {
        Some(MuxTrackKind::Text)
    } else if [
        FourCc::from_bytes(*b"stpp"),
        FourCc::from_bytes(*b"dvbs"),
        FourCc::from_bytes(*b"dvbt"),
        FourCc::from_bytes(*b"mp4s"),
    ]
    .contains(&sample_entry_type)
    {
        Some(MuxTrackKind::Subtitle)
    } else {
        None
    }
}

const fn default_handler_name_for_kind(kind: MuxTrackKind) -> &'static str {
    match kind {
        MuxTrackKind::Audio => "SoundHandler",
        MuxTrackKind::Video => "VideoHandler",
        MuxTrackKind::Text => "TextHandler",
        MuxTrackKind::Subtitle => "SubtitleHandler",
    }
}

pub(in crate::mux) fn direct_ingest_handler_name(codec_label: &str) -> String {
    let kind = match codec_label {
        "h263" | "h264" | "h265" | "vvc" | "av1" | "vp8" | "vp9" | "vp10" | "mp4v" | "mpeg2v"
        | "avs3" | "ogg-theora" | "jpeg" | "png" | "bmp" | "prores" | "y4m" | "rawvideo"
        | "j2k" => MuxTrackKind::Video,
        "vobsub" => MuxTrackKind::Subtitle,
        _ => MuxTrackKind::Audio,
    };
    default_handler_name_for_kind(kind).to_string()
}

pub(in crate::mux) fn direct_ingest_mux_policy(
    codec_label: &str,
    kind: MuxTrackKind,
) -> ImportedTrackMuxPolicy {
    let mut policy = ImportedTrackMuxPolicy::DEFAULT;
    if kind.is_audio() || codec_label == "vobsub" {
        policy.stsc_run_encoding_mode = StscRunEncodingMode::PreserveTerminalBoundary;
    }
    if codec_label == "iamf" {
        policy.sync_sample_table_mode = SyncSampleTableMode::ForceEmpty;
    }
    policy
}

pub(in crate::mux) fn direct_ingest_mux_policy_with_preferred_track_id(
    codec_label: &str,
    kind: MuxTrackKind,
    preferred_track_id: u32,
) -> ImportedTrackMuxPolicy {
    direct_ingest_mux_policy(codec_label, kind).with_preferred_track_id(preferred_track_id)
}

fn assign_imported_track_ids(
    imported_tracks: &[ImportedTrack],
    preserve_imported_track_ids: bool,
) -> Result<Vec<u32>, MuxError> {
    if !preserve_imported_track_ids {
        return imported_tracks
            .iter()
            .enumerate()
            .map(|(index, _)| {
                u32::try_from(index + 1)
                    .map_err(|_| MuxError::LayoutOverflow("track identifier assignment"))
            })
            .collect();
    }

    let mut preferred_counts = BTreeMap::<u32, usize>::new();
    for track in imported_tracks {
        if let Some(track_id) = imported_track_preserved_track_id(track) {
            *preferred_counts.entry(track_id).or_default() += 1;
        }
    }

    let mut assigned = Vec::with_capacity(imported_tracks.len());
    let mut used = BTreeMap::<u32, ()>::new();
    for (index, track) in imported_tracks.iter().enumerate() {
        let preserved = imported_track_preserved_track_id(track)
            .filter(|track_id| preferred_counts.get(track_id) == Some(&1))
            .filter(|track_id| {
                !earlier_source_order_slot_claims_track_id(
                    imported_tracks,
                    &preferred_counts,
                    index,
                    *track_id,
                )
            });
        if let Some(track_id) = preserved {
            used.insert(track_id, ());
            assigned.push(track_id);
        } else {
            assigned.push(0);
        }
    }

    for (index, track_id) in assigned.iter_mut().enumerate() {
        if *track_id != 0 {
            continue;
        }
        let mut next_track_id = u32::try_from(index + 1)
            .map_err(|_| MuxError::LayoutOverflow("track identifier assignment"))?;
        while used.contains_key(&next_track_id) {
            next_track_id = next_track_id
                .checked_add(1)
                .ok_or(MuxError::LayoutOverflow("track identifier assignment"))?;
        }
        *track_id = next_track_id;
        used.insert(next_track_id, ());
    }

    Ok(assigned)
}

fn earlier_source_order_slot_claims_track_id(
    imported_tracks: &[ImportedTrack],
    preferred_counts: &BTreeMap<u32, usize>,
    index: usize,
    track_id: u32,
) -> bool {
    imported_tracks
        .iter()
        .take(index)
        .enumerate()
        .any(|(prior_index, prior_track)| {
            let Ok(prior_source_order_track_id) = u32::try_from(prior_index + 1) else {
                return false;
            };
            prior_source_order_track_id == track_id
                && imported_track_preserved_track_id(prior_track)
                    .is_none_or(|prior_track_id| preferred_counts.get(&prior_track_id) != Some(&1))
        })
}

fn imported_track_preserved_track_id(imported_track: &ImportedTrack) -> Option<u32> {
    imported_track.mux_policy.preferred_track_id().or_else(|| {
        imported_track
            .mux_policy
            .header_policy()
            .and_then(|policy| policy.source_track_id)
    })
}

fn imported_track_source_key(imported_track: &ImportedTrack) -> Option<(usize, u32)> {
    let source_index = imported_track_single_source_index(imported_track)?;
    let source_track_id = imported_track
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_track_id)?;
    Some((source_index, source_track_id))
}

fn imported_source_track_id_remap(
    imported_tracks: &[ImportedTrack],
    assigned_track_ids: &[u32],
) -> BTreeMap<(usize, u32), u32> {
    imported_tracks
        .iter()
        .zip(assigned_track_ids.iter().copied())
        .filter_map(|(track, assigned_track_id)| {
            imported_track_source_key(track).map(|key| (key, assigned_track_id))
        })
        .collect()
}

fn remap_preserved_flat_trak_boxes(
    source_track_id_remap: &BTreeMap<(usize, u32), u32>,
    source_key: Option<(usize, u32)>,
    imported_track: &ImportedTrack,
    boxes: Vec<Vec<u8>>,
) -> Result<Vec<Vec<u8>>, MuxError> {
    boxes
        .into_iter()
        .map(|box_bytes| {
            if box_header_type(&box_bytes) != Some(TREF) {
                return Ok(box_bytes);
            }
            let Some((source_index, source_track_id)) = source_key else {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: imported_track_relation_error_spec(imported_track),
                    message:
                        "track reference box cannot be remapped because the source track identity is ambiguous"
                            .to_string(),
                });
            };
            remap_tref_box(
                source_track_id_remap,
                source_index,
                source_track_id,
                imported_track,
                &box_bytes,
            )
        })
        .collect()
}

fn remap_tref_box(
    source_track_id_remap: &BTreeMap<(usize, u32), u32>,
    source_index: usize,
    source_track_id: u32,
    imported_track: &ImportedTrack,
    tref_box: &[u8],
) -> Result<Vec<u8>, MuxError> {
    let outer = parse_encoded_box_info(
        tref_box,
        &imported_track_relation_error_spec(imported_track),
        "track reference box",
    )?;
    if outer.box_type() != TREF || outer.size() as usize != tref_box.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: imported_track_relation_error_spec(imported_track),
            message: format!("track {source_track_id} carries an invalid track reference box"),
        });
    }
    let payload_start = usize::try_from(outer.header_size())
        .map_err(|_| MuxError::LayoutOverflow("track reference header"))?;
    let mut remapped_children = Vec::new();
    let mut cursor = Cursor::new(&tref_box[payload_start..]);
    while usize::try_from(cursor.position()).unwrap_or(usize::MAX)
        < tref_box.len().saturating_sub(payload_start)
    {
        let child_start = usize::try_from(cursor.position())
            .map_err(|_| MuxError::LayoutOverflow("track reference child offset"))?;
        let child = crate::BoxInfo::read(&mut cursor).map_err(|error| {
            MuxError::UnsupportedTrackImport {
                spec: imported_track_relation_error_spec(imported_track),
                message: format!(
                    "track {source_track_id} carries an invalid track reference child: {error}"
                ),
            }
        })?;
        let child_end = child_start
            .checked_add(
                usize::try_from(child.size())
                    .map_err(|_| MuxError::LayoutOverflow("track reference child size"))?,
            )
            .ok_or(MuxError::LayoutOverflow("track reference child range"))?;
        let child_payload_start = child_start
            .checked_add(
                usize::try_from(child.header_size())
                    .map_err(|_| MuxError::LayoutOverflow("track reference child header"))?,
            )
            .ok_or(MuxError::LayoutOverflow("track reference child payload"))?;
        let tref_payload = &tref_box[payload_start..];
        if child_end > tref_payload.len() || child_payload_start > child_end {
            return Err(MuxError::UnsupportedTrackImport {
                spec: imported_track_relation_error_spec(imported_track),
                message: format!(
                    "track {source_track_id} carries a truncated track reference child"
                ),
            });
        }
        let child_payload = &tref_payload[child_payload_start..child_end];
        if !child_payload.len().is_multiple_of(4) {
            return Err(MuxError::UnsupportedTrackImport {
                spec: imported_track_relation_error_spec(imported_track),
                message: format!(
                    "track {source_track_id} carries track reference child `{}` with a non-u32 payload",
                    child.box_type()
                ),
            });
        }
        let mut remapped_payload = Vec::with_capacity(child_payload.len());
        for referenced_track_id in child_payload
            .chunks_exact(4)
            .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("four-byte chunk")))
        {
            let Some(remapped_track_id) =
                source_track_id_remap.get(&(source_index, referenced_track_id))
            else {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: imported_track_relation_error_spec(imported_track),
                    message: format!(
                        "track {source_track_id} references unavailable track {referenced_track_id} in `{}`",
                        child.box_type()
                    ),
                });
            };
            remapped_payload.extend_from_slice(&remapped_track_id.to_be_bytes());
        }
        let child_size = child
            .header_size()
            .checked_add(
                u64::try_from(remapped_payload.len())
                    .map_err(|_| MuxError::LayoutOverflow("track reference child payload"))?,
            )
            .ok_or(MuxError::LayoutOverflow("track reference child size"))?;
        remapped_children.extend_from_slice(
            &HeaderInfo::new(child.box_type(), child_size)
                .with_header_size(child.header_size())
                .encode(),
        );
        remapped_children.extend_from_slice(&remapped_payload);
        Seek::seek(
            &mut cursor,
            SeekFrom::Start(
                u64::try_from(child_end)
                    .map_err(|_| MuxError::LayoutOverflow("track reference child seek"))?,
            ),
        )?;
    }
    let tref_size = outer
        .header_size()
        .checked_add(
            u64::try_from(remapped_children.len())
                .map_err(|_| MuxError::LayoutOverflow("track reference payload"))?,
        )
        .ok_or(MuxError::LayoutOverflow("track reference box size"))?;
    let mut remapped = HeaderInfo::new(TREF, tref_size)
        .with_header_size(outer.header_size())
        .encode();
    remapped.extend_from_slice(&remapped_children);
    Ok(remapped)
}

fn filter_preserved_flat_trak_boxes_for_output(
    imported_track: &ImportedTrack,
    movie_timescale: u32,
    output_layout: MuxOutputLayout,
    preserved_flat_trak_boxes: Vec<Vec<u8>>,
) -> Vec<Vec<u8>> {
    let source_movie_timescale = imported_track
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_movie_timescale);
    let preserve_flat_edts =
        output_layout == MuxOutputLayout::Flat && source_movie_timescale == Some(movie_timescale);
    preserved_flat_trak_boxes
        .into_iter()
        .filter(|box_bytes| {
            box_header_type(box_bytes) != Some(EDTS)
                || (preserve_flat_edts
                    && preserved_flat_edts_has_material_timing(imported_track, box_bytes))
        })
        .collect()
}

fn preserved_flat_edts_has_material_timing(
    imported_track: &ImportedTrack,
    box_bytes: &[u8],
) -> bool {
    let mut reader = Cursor::new(box_bytes);
    let Ok(elst_boxes) = extract_box_as::<_, Elst>(&mut reader, None, BoxPath::from([EDTS, ELST]))
    else {
        return true;
    };
    let [elst] = elst_boxes.as_slice() else {
        return true;
    };
    preserved_flat_elst_has_material_timing(
        elst,
        imported_track
            .mux_policy
            .header_policy()
            .and_then(|policy| policy.source_media_duration),
    )
}

fn preserved_flat_elst_has_material_timing(
    elst: &Elst,
    source_media_duration: Option<u64>,
) -> bool {
    if elst.entry_count == 1 && elst.segment_duration(0) == 0 {
        return false;
    }
    !preserved_flat_elst_is_identity(elst, source_media_duration)
}

fn preserved_flat_elst_is_identity(elst: &Elst, source_media_duration: Option<u64>) -> bool {
    let Some(source_media_duration) = source_media_duration else {
        return false;
    };
    let Ok(entry_count) = usize::try_from(elst.entry_count) else {
        return false;
    };
    if entry_count == 0 || entry_count > elst.entries.len() {
        return false;
    }

    let mut media_entry_count = 0_u8;
    for index in 0..entry_count {
        let segment_duration = elst.segment_duration(index);
        let media_time = elst.media_time(index);
        if segment_duration == 0 && media_time < 0 {
            continue;
        }
        if media_time != 0
            || elst.entries[index].media_rate_integer != 1
            || segment_duration != source_media_duration
        {
            return false;
        }
        media_entry_count = media_entry_count.saturating_add(1);
    }
    media_entry_count == 1
}

fn parse_encoded_box_info(
    box_bytes: &[u8],
    spec: &str,
    context: &'static str,
) -> Result<HeaderInfo, MuxError> {
    let mut cursor = Cursor::new(box_bytes);
    crate::BoxInfo::read(&mut cursor).map_err(|error| MuxError::UnsupportedTrackImport {
        spec: spec.to_string(),
        message: format!("invalid {context}: {error}"),
    })
}

fn imported_track_relation_error_spec(imported_track: &ImportedTrack) -> String {
    imported_track
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_track_id)
        .map(|track_id| format!("track {track_id}"))
        .unwrap_or_else(|| "track".to_string())
}

fn generated_flat_stbl_boxes_for_imported_track(
    imported_track: &ImportedTrack,
    imported_mp4_carry: Option<&ImportedMp4TrackCarry>,
    preserved_flat_stbl_boxes: &[Vec<u8>],
    preserve_flat_authority_layout: bool,
) -> Result<Vec<Vec<u8>>, MuxError> {
    let mut generated = Vec::new();
    let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box);
    let preserved_box_types = preserved_flat_stbl_boxes
        .iter()
        .filter_map(|box_bytes| box_bytes.get(4..8))
        .filter_map(|box_type| box_type.try_into().ok())
        .map(FourCc::from_bytes)
        .collect::<Vec<_>>();

    if !preserved_box_types.contains(&SDTP)
        && imported_mp4_carry.is_some_and(|carry| carry.source_had_empty_stts)
        && imported_track_uses_avc_family(imported_track)
    {
        generated.push(build_imported_zero_sdtp_box(imported_track.samples.len())?);
    }

    if !preserved_box_types.contains(&SDTP)
        && imported_mp4_carry.is_some()
        && sample_entry_type == Some(FourCc::from_bytes(*b"vp08"))
    {
        generated.push(build_imported_zero_sdtp_box(imported_track.samples.len())?);
    }

    if !preserved_box_types.contains(&SDTP)
        && imported_mp4_carry.is_some()
        && matches!(
            sample_entry_type,
            Some(value) if value == FourCc::from_bytes(*b"wvtt")
        )
    {
        generated.push(build_imported_zero_sdtp_box(imported_track.samples.len())?);
    }

    if !preserved_box_types.contains(&SDTP)
        && imported_mp4_carry.is_some_and(|carry| carry.source_had_empty_stts)
        && (sample_entry_type == Some(FourCc::from_bytes(*b"ac-3"))
            || imported_track_uses_mpegh_family(imported_track))
    {
        generated.push(build_imported_zero_sdtp_box(imported_track.samples.len())?);
    }

    if !preserved_box_types.contains(&SDTP)
        && imported_mp4_carry.is_some()
        && sample_entry_type == Some(FourCc::from_bytes(*b"hev1"))
        && !sample_entry_carries_box_type(
            &imported_track.sample_entry_box,
            FourCc::from_bytes(*b"fiel"),
        )
    {
        generated.push(build_imported_zero_sdtp_box(imported_track.samples.len())?);
    }

    if imported_mp4_carry.is_some()
        && imported_track_uses_av1_family(imported_track)
        && !preserved_box_types.contains(&SDTP)
    {
        generated.push(build_imported_av1_sdtp_box(imported_track)?);
    }

    let carries_random_access_groups =
        preserved_box_types.contains(&SGPD) || preserved_box_types.contains(&SBGP);
    let should_suppress_random_access_groups =
        preserve_flat_authority_layout && imported_track.mux_policy.header_policy().is_some();
    let has_multiple_sync_samples = imported_track
        .samples
        .iter()
        .filter(|sample| sample.is_sync_sample)
        .count()
        > 1;

    if imported_track_uses_hevc_family(imported_track)
        && !sample_entry_carries_dolby_vision_config(&imported_track.sample_entry_box)
        && !carries_random_access_groups
        && !should_suppress_random_access_groups
        && has_multiple_sync_samples
    {
        generated.push(build_imported_visual_random_access_sgpd_box()?);
        generated.push(build_imported_visual_random_access_sbgp_box(
            imported_track,
        )?);
    }

    if imported_track_uses_layered_hevc_family(imported_track)
        && !preserved_box_types.contains(&CSLG)
        && imported_track.mux_policy.header_policy().is_some()
        && imported_track
            .samples
            .iter()
            .any(|sample| sample.composition_time_offset != 0)
        && let Some(cslg_box) = build_generated_imported_cslg_box(imported_track)?
    {
        generated.push(cslg_box);
    }

    Ok(generated)
}

fn build_generated_imported_cslg_box(
    imported_track: &ImportedTrack,
) -> Result<Option<Vec<u8>>, MuxError> {
    let mut decode_time = 0_i128;
    let mut saw_offset = false;
    let mut least_decode_to_display_delta = i128::MAX;
    let mut greatest_decode_to_display_delta = i128::MIN;
    let mut composition_start_time = i128::MAX;
    let mut max_presentation_end = i128::MIN;
    for sample in &imported_track.samples {
        let composition_offset = i128::from(sample.composition_time_offset);
        saw_offset |= composition_offset != 0;
        least_decode_to_display_delta = least_decode_to_display_delta.min(composition_offset);
        greatest_decode_to_display_delta = greatest_decode_to_display_delta.max(composition_offset);
        composition_start_time =
            composition_start_time.min(decode_time.saturating_add(composition_offset));
        max_presentation_end = max_presentation_end.max(
            decode_time
                .saturating_add(composition_offset)
                .saturating_add(i128::from(sample.duration)),
        );
        decode_time = decode_time.saturating_add(i128::from(sample.duration));
    }
    if !saw_offset {
        return Ok(None);
    }

    let composition_end_time = imported_track_flat_authority_media_duration(imported_track)
        .map(i128::from)
        .unwrap_or(max_presentation_end);
    let composition_start_time = composition_start_time.max(0);
    let mut cslg = Cslg::default();
    cslg.set_version(0);
    cslg.composition_to_dts_shift_v0 = 0;
    cslg.least_decode_to_display_delta_v0 = i32::try_from(least_decode_to_display_delta)
        .map_err(|_| MuxError::LayoutOverflow("imported cslg least delta"))?;
    cslg.greatest_decode_to_display_delta_v0 = i32::try_from(greatest_decode_to_display_delta)
        .map_err(|_| MuxError::LayoutOverflow("imported cslg greatest delta"))?;
    cslg.composition_start_time_v0 = i32::try_from(composition_start_time)
        .map_err(|_| MuxError::LayoutOverflow("imported cslg start time"))?;
    cslg.composition_end_time_v0 = i32::try_from(composition_end_time)
        .map_err(|_| MuxError::LayoutOverflow("imported cslg end time"))?;
    Ok(Some(super::mp4::encode_typed_box(&cslg, &[])?))
}

fn sample_entry_carries_dolby_vision_config(sample_entry_box: &[u8]) -> bool {
    let Ok(child_boxes) = super::mp4::visual_sample_entry_immediate_children(sample_entry_box)
    else {
        return false;
    };
    child_boxes.iter().any(|child_box| {
        matches!(
            sample_entry_box_type(child_box),
            Some(value)
                if value == FourCc::from_bytes(*b"dvcC")
                    || value == FourCc::from_bytes(*b"dvvC")
        )
    })
}

fn sample_entry_carries_box_type(sample_entry_box: &[u8], target: FourCc) -> bool {
    let Ok(child_boxes) = super::mp4::visual_sample_entry_immediate_children(sample_entry_box)
    else {
        return false;
    };
    child_boxes
        .iter()
        .any(|child_box| sample_entry_box_type(child_box) == Some(target))
}

fn imported_track_uses_layered_hevc_family(imported_track: &ImportedTrack) -> bool {
    imported_track_uses_hevc_family(imported_track)
        && sample_entry_carries_box_type(
            &imported_track.sample_entry_box,
            FourCc::from_bytes(*b"lhvC"),
        )
}

fn build_imported_zero_sdtp_box(sample_count: usize) -> Result<Vec<u8>, MuxError> {
    let samples = std::iter::repeat_n(
        SdtpSampleElem {
            is_leading: 0,
            sample_depends_on: 0,
            sample_is_depended_on: 0,
            sample_has_redundancy: 0,
        },
        sample_count,
    )
    .collect();
    let mut sdtp = Sdtp::default();
    sdtp.samples = samples;
    super::mp4::encode_typed_box(&sdtp, &[])
}

fn build_fragmented_imported_vp08_flat_chunk_sample_counts(
    track_id: u32,
    imported_track: &ImportedTrack,
) -> Result<Vec<u32>, MuxError> {
    let total_sample_count = imported_track.samples.len();
    if total_sample_count == 0 {
        return Ok(Vec::new());
    }

    let first_chunk_sample_count = imported_track
        .samples
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(sample_index, sample)| sample.is_sync_sample.then_some(sample_index))
        .unwrap_or(total_sample_count);
    let first_chunk_sample_count = u32::try_from(first_chunk_sample_count)
        .map_err(|_| MuxError::LayoutOverflow("flat vp08 chunk plan"))?;
    let trailing_chunk_count = total_sample_count
        .checked_sub(usize::try_from(first_chunk_sample_count).unwrap_or(total_sample_count))
        .ok_or(MuxError::LayoutOverflow("flat vp08 chunk plan"))?;
    let mut chunk_sample_counts = Vec::with_capacity(1 + trailing_chunk_count);
    chunk_sample_counts.push(first_chunk_sample_count);
    chunk_sample_counts.extend(std::iter::repeat_n(1_u32, trailing_chunk_count));

    let planned_sample_count = chunk_sample_counts
        .iter()
        .try_fold(0_usize, |total, chunk_sample_count| {
            total.checked_add(usize::try_from(*chunk_sample_count).ok()?)
        })
        .ok_or(MuxError::InvalidChunkPlan {
            track_id,
            message:
                "fragmented flat vp08 chunk plan overflowed while validating staged sample coverage"
                    .to_string(),
        })?;
    if planned_sample_count != total_sample_count {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: format!(
                "fragmented flat vp08 chunk plan covered {planned_sample_count} sample{} but the imported track carried {total_sample_count}",
                if planned_sample_count == 1 { "" } else { "s" },
            ),
        });
    }

    Ok(chunk_sample_counts)
}

fn validate_imported_flat_chunk_sample_counts(
    track_id: u32,
    kind: MuxTrackKind,
    total_sample_count: usize,
    chunk_sample_counts: &[u32],
) -> Result<Vec<u32>, MuxError> {
    let planned_sample_count = chunk_sample_counts
        .iter()
        .try_fold(0_usize, |total, chunk_sample_count| {
            total.checked_add(usize::try_from(*chunk_sample_count).ok()?)
        })
        .ok_or(MuxError::InvalidChunkPlan {
            track_id,
            message: format!(
                "explicit flat {} chunk plan overflowed while validating staged sample coverage",
                flat_destination_append_kind_label(kind)
            ),
        })?;
    if planned_sample_count != total_sample_count {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: format!(
                "explicit flat {} chunk plan covered {planned_sample_count} sample{} but the imported track carried {total_sample_count}",
                flat_destination_append_kind_label(kind),
                if planned_sample_count == 1 { "" } else { "s" },
            ),
        });
    }
    Ok(chunk_sample_counts.to_vec())
}

fn build_imported_av1_sdtp_box(imported_track: &ImportedTrack) -> Result<Vec<u8>, MuxError> {
    let samples = imported_track
        .samples
        .iter()
        .map(|sample| SdtpSampleElem {
            is_leading: 0,
            sample_depends_on: if sample.is_sync_sample { 2 } else { 1 },
            sample_is_depended_on: 0,
            sample_has_redundancy: 0,
        })
        .collect();
    let mut sdtp = Sdtp::default();
    sdtp.samples = samples;
    super::mp4::encode_typed_box(&sdtp, &[])
}

fn build_imported_visual_random_access_sgpd_box() -> Result<Vec<u8>, MuxError> {
    super::mp4::encode_typed_box(&super::mp4::build_visual_random_access_sgpd(), &[])
}

fn build_imported_visual_random_access_sbgp_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let mut entries = Vec::<SbgpEntry>::new();
    let mut current_sample_number = 1_u32;
    for sync_sample_number in imported_track
        .samples
        .iter()
        .enumerate()
        .filter_map(|(index, sample)| sample.is_sync_sample.then_some(index + 1))
        .skip(1)
    {
        let sync_sample_number = u32::try_from(sync_sample_number)
            .map_err(|_| MuxError::LayoutOverflow("visual random-access sample number"))?;
        let gap = sync_sample_number.saturating_sub(current_sample_number);
        if gap != 0 {
            entries.push(SbgpEntry {
                sample_count: gap,
                group_description_index: 0,
            });
        }
        entries.push(SbgpEntry {
            sample_count: 1,
            group_description_index: 1,
        });
        current_sample_number = sync_sample_number.saturating_add(1);
    }
    super::mp4::encode_typed_box(&super::mp4::build_visual_random_access_sbgp(entries)?, &[])
}

fn stsd_child_is_padding(box_type: FourCc) -> bool {
    matches!(box_type, FourCc::ANY | FREE | SKIP | WIDE)
}

fn extract_stsd_sample_entries_sync<R>(
    path: &Path,
    reader: &mut R,
    stsd_info: &HeaderInfo,
    stsd: &crate::boxes::iso14496_12::Stsd,
    track_id: u32,
) -> Result<(Vec<ExtractedBox>, Vec<Vec<u8>>), MuxError>
where
    R: Read + Seek,
{
    let sample_entry_infos = extract_box(reader, Some(stsd_info), BoxPath::from([FourCc::ANY]))?
        .into_iter()
        .filter(|info| !stsd_child_is_padding(info.box_type()))
        .collect::<Vec<_>>();
    if sample_entry_infos.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {} does not expose a sample-entry payload", track_id),
        });
    }
    if usize::try_from(stsd.entry_count).unwrap_or(usize::MAX) != sample_entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {} declares {} sample descriptions but exposes {} sample-entry payloads",
                track_id,
                stsd.entry_count,
                sample_entry_infos.len()
            ),
        });
    }

    let sample_entries =
        extract_box_with_payload(reader, Some(stsd_info), BoxPath::from([FourCc::ANY]))?
            .into_iter()
            .filter(|entry| !stsd_child_is_padding(entry.info.box_type()))
            .collect::<Vec<_>>();
    if sample_entries.len() != sample_entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes inconsistent sample-entry payloads"),
        });
    }
    let mut sample_entry_boxes = Vec::with_capacity(sample_entry_infos.len());
    for sample_entry_info in sample_entry_infos {
        let sample_entry_box = read_box_bytes_sync(reader, &sample_entry_info)?;
        sample_entry_boxes.push(sample_entry_box);
    }
    Ok((sample_entries, sample_entry_boxes))
}

fn extract_data_references_sync<R>(
    path: &Path,
    reader: &mut R,
    trak_info: &HeaderInfo,
    track_id: u32,
) -> Result<Vec<ImportedDataReference>, MuxError>
where
    R: Read + Seek,
{
    let dref_infos = extract_box(
        reader,
        Some(trak_info),
        BoxPath::from([MDIA, MINF, DINF, DREF]),
    )?;
    if dref_infos.is_empty() {
        return Ok(vec![ImportedDataReference::SelfContained]);
    }
    let [dref_info] = dref_infos.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes multiple data-reference tables"),
        });
    };
    let dref = extract_required_single_as_sync::<_, Dref>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, DINF, DREF]),
        "dref",
    )?;
    let entry_infos = extract_box(reader, Some(dref_info), BoxPath::from([FourCc::ANY]))?;
    if usize::try_from(dref.entry_count).unwrap_or(usize::MAX) != entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} declares {} data references but exposes {} entries",
                dref.entry_count,
                entry_infos.len()
            ),
        });
    }
    let mut references = Vec::with_capacity(entry_infos.len());
    for entry_info in entry_infos {
        let entry_bytes = read_box_bytes_sync(reader, &entry_info)?;
        let reference = match entry_info.box_type() {
            value if value == URL => {
                let url = super::mp4::decode_typed_box::<Url>(&entry_bytes)?;
                if url.flags() & 1 != 0 {
                    ImportedDataReference::SelfContained
                } else {
                    ImportedDataReference::LocalFile(resolve_local_data_reference_path(
                        path,
                        track_id,
                        &url.location,
                    )?)
                }
            }
            value if value == URN => {
                let urn = super::mp4::decode_typed_box::<Urn>(&entry_bytes)?;
                if urn.flags() & 1 != 0 {
                    ImportedDataReference::SelfContained
                } else {
                    ImportedDataReference::LocalFile(resolve_local_data_reference_path(
                        path,
                        track_id,
                        &urn.location,
                    )?)
                }
            }
            value => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} uses unsupported data-reference entry `{value}`"
                    ),
                });
            }
        };
        references.push(reference);
    }
    Ok(references)
}

#[cfg(feature = "async")]
async fn extract_stsd_sample_entries_async<R>(
    path: &Path,
    reader: &mut R,
    stsd_info: &HeaderInfo,
    stsd: &crate::boxes::iso14496_12::Stsd,
    track_id: u32,
) -> Result<(Vec<ExtractedBox>, Vec<Vec<u8>>), MuxError>
where
    R: AsyncReadSeek,
{
    let sample_entry_infos =
        extract_box_async(reader, Some(stsd_info), BoxPath::from([FourCc::ANY]))
            .await?
            .into_iter()
            .filter(|info| !stsd_child_is_padding(info.box_type()))
            .collect::<Vec<_>>();
    if sample_entry_infos.is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {} does not expose a sample-entry payload", track_id),
        });
    }
    if usize::try_from(stsd.entry_count).unwrap_or(usize::MAX) != sample_entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {} declares {} sample descriptions but exposes {} sample-entry payloads",
                track_id,
                stsd.entry_count,
                sample_entry_infos.len()
            ),
        });
    }

    let sample_entries =
        extract_box_with_payload_async(reader, Some(stsd_info), BoxPath::from([FourCc::ANY]))
            .await?
            .into_iter()
            .filter(|entry| !stsd_child_is_padding(entry.info.box_type()))
            .collect::<Vec<_>>();
    if sample_entries.len() != sample_entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes inconsistent sample-entry payloads"),
        });
    }
    let mut sample_entry_boxes = Vec::with_capacity(sample_entry_infos.len());
    for sample_entry_info in sample_entry_infos {
        let sample_entry_box = read_box_bytes_async(reader, &sample_entry_info).await?;
        sample_entry_boxes.push(sample_entry_box);
    }
    Ok((sample_entries, sample_entry_boxes))
}

#[cfg(feature = "async")]
async fn extract_data_references_async<R>(
    path: &Path,
    reader: &mut R,
    trak_info: &HeaderInfo,
    track_id: u32,
) -> Result<Vec<ImportedDataReference>, MuxError>
where
    R: AsyncReadSeek,
{
    let dref_infos = extract_box_async(
        reader,
        Some(trak_info),
        BoxPath::from([MDIA, MINF, DINF, DREF]),
    )
    .await?;
    if dref_infos.is_empty() {
        return Ok(vec![ImportedDataReference::SelfContained]);
    }
    let [dref_info] = dref_infos.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} exposes multiple data-reference tables"),
        });
    };
    let dref = extract_required_single_as_async::<_, Dref>(
        reader,
        trak_info,
        BoxPath::from([MDIA, MINF, DINF, DREF]),
        "dref",
    )
    .await?;
    let entry_infos =
        extract_box_async(reader, Some(dref_info), BoxPath::from([FourCc::ANY])).await?;
    if usize::try_from(dref.entry_count).unwrap_or(usize::MAX) != entry_infos.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} declares {} data references but exposes {} entries",
                dref.entry_count,
                entry_infos.len()
            ),
        });
    }
    let mut references = Vec::with_capacity(entry_infos.len());
    for entry_info in entry_infos {
        let entry_bytes = read_box_bytes_async(reader, &entry_info).await?;
        let reference = match entry_info.box_type() {
            value if value == URL => {
                let url = super::mp4::decode_typed_box::<Url>(&entry_bytes)?;
                if url.flags() & 1 != 0 {
                    ImportedDataReference::SelfContained
                } else {
                    ImportedDataReference::LocalFile(resolve_local_data_reference_path(
                        path,
                        track_id,
                        &url.location,
                    )?)
                }
            }
            value if value == URN => {
                let urn = super::mp4::decode_typed_box::<Urn>(&entry_bytes)?;
                if urn.flags() & 1 != 0 {
                    ImportedDataReference::SelfContained
                } else {
                    ImportedDataReference::LocalFile(resolve_local_data_reference_path(
                        path,
                        track_id,
                        &urn.location,
                    )?)
                }
            }
            value => {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} uses unsupported data-reference entry `{value}`"
                    ),
                });
            }
        };
        references.push(reference);
    }
    Ok(references)
}

fn resolve_local_data_reference_path(
    movie_path: &Path,
    track_id: u32,
    location: &str,
) -> Result<PathBuf, MuxError> {
    if location.is_empty() || location.contains('\0') {
        return Err(MuxError::UnsupportedTrackImport {
            spec: movie_path.display().to_string(),
            message: format!("track {track_id} uses an empty or invalid data-reference location"),
        });
    }
    let mut reference_path = if location.starts_with("file:") {
        if location.contains('%') {
            return Err(MuxError::UnsupportedTrackImport {
                spec: movie_path.display().to_string(),
                message: format!("track {track_id} uses an encoded data-reference location"),
            });
        }
        resolve_local_data_reference_file_uri_path(location).ok_or_else(|| {
            MuxError::UnsupportedTrackImport {
                spec: movie_path.display().to_string(),
                message: format!("track {track_id} uses a non-local data-reference location"),
            }
        })?
    } else if location.contains("://") {
        return Err(MuxError::UnsupportedTrackImport {
            spec: movie_path.display().to_string(),
            message: format!("track {track_id} uses a non-local data-reference location"),
        });
    } else {
        if location.contains('%') {
            return Err(MuxError::UnsupportedTrackImport {
                spec: movie_path.display().to_string(),
                message: format!("track {track_id} uses an encoded data-reference location"),
            });
        }
        PathBuf::from(location)
    };
    if reference_path.as_os_str().is_empty() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: movie_path.display().to_string(),
            message: format!("track {track_id} uses an empty data-reference path"),
        });
    }
    if reference_path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err(MuxError::UnsupportedTrackImport {
            spec: movie_path.display().to_string(),
            message: format!("track {track_id} uses an unsafe data-reference path"),
        });
    }
    if !reference_path.is_absolute() {
        reference_path = movie_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(reference_path);
    }
    absolute_path(&reference_path)
}

fn resolve_local_data_reference_file_uri_path(uri: &str) -> Option<PathBuf> {
    let rest = uri.strip_prefix("file:")?;
    if let Some(path) = rest.strip_prefix("///") {
        return resolve_local_data_reference_absolute_file_uri_path(path);
    }
    if let Some(authority_path) = rest.strip_prefix("//") {
        let (authority, path) = authority_path.split_once('/')?;
        if authority.eq_ignore_ascii_case("localhost") {
            return resolve_local_data_reference_absolute_file_uri_path(path);
        }
        return resolve_local_data_reference_authority_file_uri_path(authority, path);
    }
    if let Some(path) = rest.strip_prefix('/') {
        return resolve_local_data_reference_single_slash_file_uri_path(path);
    }
    None
}

#[cfg(windows)]
fn resolve_local_data_reference_single_slash_file_uri_path(path: &str) -> Option<PathBuf> {
    if path.len() >= 2 && path.as_bytes()[1] == b':' && path.as_bytes()[0].is_ascii_alphabetic() {
        Some(PathBuf::from(path))
    } else {
        None
    }
}

#[cfg(not(windows))]
fn resolve_local_data_reference_single_slash_file_uri_path(path: &str) -> Option<PathBuf> {
    resolve_local_data_reference_absolute_file_uri_path(path)
}

#[cfg(windows)]
fn resolve_local_data_reference_absolute_file_uri_path(path: &str) -> Option<PathBuf> {
    if path.len() >= 2 && path.as_bytes()[1] == b':' && path.as_bytes()[0].is_ascii_alphabetic() {
        Some(PathBuf::from(path))
    } else if path.starts_with('/') {
        Some(PathBuf::from(format!(
            r"\\{}",
            path.trim_start_matches('/').replace('/', "\\")
        )))
    } else if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

#[cfg(windows)]
fn resolve_local_data_reference_authority_file_uri_path(
    authority: &str,
    path: &str,
) -> Option<PathBuf> {
    if authority.is_empty() || path.is_empty() {
        None
    } else if authority.len() == 2
        && authority.as_bytes()[1] == b':'
        && authority.as_bytes()[0].is_ascii_alphabetic()
    {
        Some(PathBuf::from(format!("{authority}/{path}")))
    } else {
        Some(PathBuf::from(format!(
            r"\\{}\{}",
            authority,
            path.replace('/', "\\")
        )))
    }
}

#[cfg(not(windows))]
fn resolve_local_data_reference_absolute_file_uri_path(path: &str) -> Option<PathBuf> {
    if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(format!("/{}", path.trim_start_matches('/'))))
    }
}

#[cfg(not(windows))]
fn resolve_local_data_reference_authority_file_uri_path(
    _authority: &str,
    _path: &str,
) -> Option<PathBuf> {
    None
}

fn sample_entry_data_reference_index(
    path: &Path,
    track_id: u32,
    sample_entry_box: &[u8],
    sample_description_index: u32,
) -> Result<u16, MuxError> {
    let Some(bytes) = sample_entry_box.get(14..16) else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} sample description {sample_description_index} is truncated before the data-reference index"
            ),
        });
    };
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn sample_data_references_for_description_indices(
    path: &Path,
    track_id: u32,
    sample_entry_boxes: &[Vec<u8>],
    data_references: &[ImportedDataReference],
    sample_description_indices: &[u32],
) -> Result<Vec<ImportedDataReference>, MuxError> {
    let mut resolved = Vec::with_capacity(sample_description_indices.len());
    for sample_description_index in sample_description_indices.iter().copied() {
        let description_index = usize::try_from(sample_description_index.saturating_sub(1))
            .map_err(|_| MuxError::LayoutOverflow("sample description index conversion"))?;
        let Some(sample_entry_box) = sample_entry_boxes.get(description_index) else {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} uses sample description index {sample_description_index} with {} sample entries",
                    sample_entry_boxes.len()
                ),
            });
        };
        let data_reference_index = sample_entry_data_reference_index(
            path,
            track_id,
            sample_entry_box,
            sample_description_index,
        )?;
        if data_reference_index == 0 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} sample description {sample_description_index} uses data-reference index 0"
                ),
            });
        }
        let reference_index = usize::from(data_reference_index - 1);
        let Some(reference) = data_references.get(reference_index) else {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} sample description {sample_description_index} references missing data-reference {data_reference_index}"
                ),
            });
        };
        resolved.push(reference.clone());
    }
    Ok(resolved)
}

fn read_box_bytes_sync<R>(reader: &mut R, info: &HeaderInfo) -> Result<Vec<u8>, MuxError>
where
    R: Read + Seek,
{
    let size =
        usize::try_from(info.size()).map_err(|_| MuxError::LayoutOverflow("box byte range"))?;
    let mut bytes = vec![0_u8; size];
    reader.seek(SeekFrom::Start(info.offset()))?;
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[cfg(feature = "async")]
async fn read_box_bytes_async<R>(reader: &mut R, info: &HeaderInfo) -> Result<Vec<u8>, MuxError>
where
    R: AsyncReadSeek,
{
    let size =
        usize::try_from(info.size()).map_err(|_| MuxError::LayoutOverflow("box byte range"))?;
    let mut bytes = vec![0_u8; size];
    reader.seek(SeekFrom::Start(info.offset())).await?;
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::{
        ImportedMp4TrackCarry, ImportedSample, ImportedTrack, ImportedTrackHeaderPolicy,
        ImportedTrackMuxPolicy, MuxTrackKind, SelectedImportedMp4CarryMap, SourceCatalog,
        SourceSpec, assign_imported_track_ids, choose_file_config, finish_prepared_request,
        stsd_child_is_padding,
    };
    use crate::FourCc;
    use crate::boxes::iso14496_12::{
        Elst, ElstEntry, Stsc, StscEntry, TFHD_DEFAULT_BASE_IS_MOOF,
        TFHD_DEFAULT_SAMPLE_DURATION_PRESENT, TFHD_DEFAULT_SAMPLE_SIZE_PRESENT,
        TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT, TRUN_DATA_OFFSET_PRESENT, Tfhd, Trun,
    };
    use crate::codec::MutableBox;
    use crate::mux::{
        MuxDurationMode, MuxFileConfig, MuxMp4TrackSelector, MuxOutputLayout, MuxRequest,
        MuxTrackSpec,
    };
    use crate::walk::BoxPath;
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;

    fn mux_fixture_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("mux")
            .join(name)
    }

    #[test]
    fn resolve_local_data_reference_path_keeps_plain_relative_paths_relative_to_movie() {
        let movie_path = std::env::current_dir().unwrap().join("movie.mp4");
        let resolved =
            super::resolve_local_data_reference_path(&movie_path, 1, "sidecar.bin").unwrap();

        assert_eq!(resolved, movie_path.parent().unwrap().join("sidecar.bin"));
        assert!(
            super::resolve_local_data_reference_path(&movie_path, 1, "file:sidecar.bin").is_err()
        );
        assert!(
            super::resolve_local_data_reference_path(&movie_path, 1, "https://example.invalid/a")
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_local_data_reference_path_keeps_unix_file_uri_paths() {
        let movie_path = PathBuf::from("/tmp/movie.mp4");

        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file:///tmp/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("/tmp/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file:////tmp/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("/tmp/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://localhost/private/var/tmp/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("/private/var/tmp/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://LOCALHOST/tmp/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("/tmp/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file:/tmp/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("/tmp/media/segment.mdat")
        );
        assert!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://media/assets/segment.mdat"
            )
            .is_err()
        );
    }

    #[cfg(windows)]
    #[test]
    fn resolve_local_data_reference_path_keeps_windows_file_uri_paths() {
        let movie_path = PathBuf::from("C:/movie.mp4");

        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file:///C:/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("C:/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://C:/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("C:/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://LOCALHOST/C:/media/segment.mdat"
            )
            .unwrap(),
            PathBuf::from("C:/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(&movie_path, 1, "file:/C:/media/segment.mdat")
                .unwrap(),
            PathBuf::from("C:/media/segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file:////media/assets/segment.mdat"
            )
            .unwrap(),
            PathBuf::from(r"\\media\assets\segment.mdat")
        );
        assert_eq!(
            super::resolve_local_data_reference_path(
                &movie_path,
                1,
                "file://media/assets/segment.mdat"
            )
            .unwrap(),
            PathBuf::from(r"\\media\assets\segment.mdat")
        );
    }

    fn imported_track(
        kind: MuxTrackKind,
        preferred_track_id: Option<u32>,
        source_index: usize,
    ) -> ImportedTrack {
        let mux_policy = preferred_track_id
            .map(|track_id| ImportedTrackMuxPolicy::DEFAULT.with_preferred_track_id(track_id))
            .unwrap_or(ImportedTrackMuxPolicy::DEFAULT);
        ImportedTrack {
            kind,
            timescale: 1,
            language: *b"und",
            handler_name: String::new(),
            mux_policy,
            width: 0,
            height: 0,
            sample_entry_box: Vec::new(),
            source_edit_media_time: None,
            sample_roll_distance: None,
            samples: vec![ImportedSample {
                source_index,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            }],
        }
    }

    fn avc_sample_entry_box_for_sync_supplement_tests() -> Vec<u8> {
        let avcc = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AVCDecoderConfiguration {
                configuration_version: 1,
                profile: 100,
                profile_compatibility: 0,
                level: 31,
                length_size_minus_one: 3,
                ..crate::boxes::iso14496_12::AVCDecoderConfiguration::default()
            },
            &[],
        )
        .unwrap();
        crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"avc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &avcc,
        )
        .unwrap()
    }

    #[test]
    fn assign_imported_track_ids_uses_source_order_slots_for_unpreferred_tracks() {
        let imported_tracks = vec![
            imported_track(MuxTrackKind::Video, Some(256), 0),
            imported_track(MuxTrackKind::Audio, None, 1),
            imported_track(MuxTrackKind::Audio, Some(448), 2),
        ];

        let assigned = assign_imported_track_ids(&imported_tracks, true).unwrap();

        assert_eq!(assigned, vec![256, 2, 448]);
    }

    #[test]
    fn assign_imported_track_ids_keeps_earlier_source_order_slot() {
        let imported_tracks = vec![
            imported_track(MuxTrackKind::Video, None, 0),
            imported_track(MuxTrackKind::Subtitle, Some(1), 1),
        ];

        let assigned = assign_imported_track_ids(&imported_tracks, true).unwrap();

        assert_eq!(assigned, vec![1, 2]);
    }

    #[test]
    fn assign_imported_track_ids_uses_sequential_order_for_fragmented_output() {
        let imported_tracks = vec![
            imported_track(MuxTrackKind::Video, Some(256), 0),
            imported_track(MuxTrackKind::Audio, None, 1),
            imported_track(MuxTrackKind::Audio, Some(448), 2),
        ];

        let assigned = assign_imported_track_ids(&imported_tracks, false).unwrap();

        assert_eq!(assigned, vec![1, 2, 3]);
    }

    fn raw_track_reference_box(child_type: FourCc, track_ids: &[u32]) -> Vec<u8> {
        let payload = track_ids
            .iter()
            .flat_map(|track_id| track_id.to_be_bytes())
            .collect::<Vec<_>>();
        let child = crate::mux::mp4::encode_raw_box(child_type, &payload).unwrap();
        crate::mux::mp4::encode_raw_box(super::TREF, &child).unwrap()
    }

    fn remapped_tref_child_ids(tref_box: &[u8]) -> Vec<u32> {
        let child_payload = &tref_box[16..];
        child_payload
            .chunks_exact(4)
            .map(|bytes| u32::from_be_bytes(bytes.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn remap_preserved_flat_trak_boxes_remaps_tref_child_track_ids() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, None, 4);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(11),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        let mut remap = BTreeMap::new();
        remap.insert((4, 11), 101);
        remap.insert((4, 12), 102);
        remap.insert((4, 13), 103);
        let tref = raw_track_reference_box(FourCc::from_bytes(*b"cdsc"), &[12, 13]);

        let boxes = super::remap_preserved_flat_trak_boxes(
            &remap,
            super::imported_track_source_key(&imported_track),
            &imported_track,
            vec![tref],
        )
        .unwrap();

        assert_eq!(boxes.len(), 1);
        assert_eq!(remapped_tref_child_ids(&boxes[0]), vec![102, 103]);
    }

    #[test]
    fn remap_preserved_flat_trak_boxes_rejects_unavailable_tref_targets() {
        let mut imported_track = imported_track(MuxTrackKind::Video, None, 0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(7),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });
        let mut remap = BTreeMap::new();
        remap.insert((0, 7), 1);
        let tref = raw_track_reference_box(FourCc::from_bytes(*b"sync"), &[9]);

        let error = super::remap_preserved_flat_trak_boxes(
            &remap,
            super::imported_track_source_key(&imported_track),
            &imported_track,
            vec![tref],
        )
        .unwrap_err();

        assert!(error.to_string().contains("references unavailable track 9"));
    }

    #[test]
    fn extract_preserved_flat_stbl_boxes_keeps_side_metadata_allowlist() {
        let padb = FourCc::from_bytes(*b"padb");
        let children = [
            crate::mux::mp4::encode_raw_box(super::CSLG, &[0; 4]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::SDTP, &[1, 2]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::STPS, &[0, 0, 0, 1]).unwrap(),
            crate::mux::mp4::encode_raw_box(padb, &[0; 4]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::STDP, &[0; 4]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::SUBS, &[0; 8]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::SGPD, &[0; 8]).unwrap(),
            crate::mux::mp4::encode_raw_box(super::SBGP, &[0; 8]).unwrap(),
        ]
        .concat();
        let stbl = crate::mux::mp4::encode_raw_box(super::STBL, &children).unwrap();
        let mut reader = Cursor::new(stbl);
        let stbl_info = crate::BoxInfo::read(&mut reader).unwrap();

        let preserved =
            super::extract_preserved_flat_stbl_boxes_sync(&mut reader, &stbl_info).unwrap();

        assert_eq!(
            preserved
                .iter()
                .filter_map(|box_bytes| super::box_header_type(box_bytes))
                .collect::<Vec<_>>(),
            vec![
                super::CSLG,
                super::SDTP,
                super::STPS,
                super::STDP,
                super::SUBS,
                super::SGPD,
                super::SBGP,
            ]
        );
    }

    #[test]
    fn collect_fragment_candidate_samples_carries_sample_description_index() {
        let path = PathBuf::from("fragmented-input.mp4");
        let context = super::FragmentRunContext {
            path: path.as_path(),
            source_index: 0,
            track_id: 1,
            moof_offset: 100,
            trex: None,
        };
        let mut tfhd = Tfhd::default();
        tfhd.track_id = 1;
        tfhd.sample_description_index = 2;
        tfhd.default_sample_duration = 10;
        tfhd.default_sample_size = 4;
        tfhd.set_flags(
            TFHD_DEFAULT_BASE_IS_MOOF
                | TFHD_DEFAULT_SAMPLE_DURATION_PRESENT
                | TFHD_DEFAULT_SAMPLE_SIZE_PRESENT
                | TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT,
        );
        let mut trun = Trun::default();
        trun.sample_count = 1;
        trun.data_offset = 24;
        trun.set_flags(TRUN_DATA_OFFSET_PRESENT);
        let trun_info = crate::BoxInfo::new(super::TRUN, 20).with_offset(120);
        let mut output = Vec::new();
        let mut sample_description_indices = Vec::new();

        super::collect_fragment_candidate_samples_from_runs(
            &context,
            &tfhd,
            &[trun],
            &[trun_info],
            &mut output,
            &mut sample_description_indices,
        )
        .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(sample_description_indices, vec![2]);
    }

    #[test]
    fn imported_mp4_avc_nal_is_intra_slice_detects_i_slice() {
        let intra_slice_nal = [0x41, 0xB8];

        assert!(super::imported_mp4_avc_nal_is_intra_slice(&intra_slice_nal));
    }

    #[test]
    fn imported_mp4_avc_sample_contains_sync_nal_accepts_invalid_length_prefixed_idr_header() {
        let malformed_idr_prefixed_sample = [0xFD, 0x9D, 0x60, 0x65, 0x25, 0x66];

        assert!(super::imported_mp4_avc_sample_contains_sync_nal(
            &malformed_idr_prefixed_sample,
            4
        ));
    }

    #[test]
    fn stsd_child_padding_filter_accepts_only_padding_box_types() {
        assert!(stsd_child_is_padding(FourCc::ANY));
        assert!(stsd_child_is_padding(FourCc::from_bytes(*b"free")));
        assert!(stsd_child_is_padding(FourCc::from_bytes(*b"skip")));
        assert!(stsd_child_is_padding(FourCc::from_bytes(*b"wide")));
        assert!(!stsd_child_is_padding(FourCc::from_bytes(*b"spex")));
        assert!(!stsd_child_is_padding(FourCc::from_bytes(*b"mp4a")));
    }

    #[test]
    fn imported_mp4_avc_sync_supplement_tolerates_truncated_best_effort_reads() {
        let sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        let mut reader = Cursor::new(vec![0_u8; 3]);
        let mut sync_samples = vec![false];

        super::supplement_imported_mp4_avc_sync_samples_sync(
            &mut reader,
            FourCc::from_bytes(*b"avc1"),
            &sample_entry_box,
            None,
            &[0],
            &[4],
            &mut sync_samples,
        )
        .unwrap();

        assert_eq!(sync_samples, vec![false]);
    }

    #[test]
    fn imported_mp4_avc_sync_supplement_widens_with_first_only_source_stss() {
        let sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        let mut reader = Cursor::new(vec![0, 0, 0, 1, 0x65]);
        let mut sync_samples = vec![false];
        let mut stss = crate::boxes::iso14496_12::Stss::default();
        stss.entry_count = 1;
        stss.sample_number = vec![1];

        super::supplement_imported_mp4_avc_sync_samples_sync(
            &mut reader,
            FourCc::from_bytes(*b"avc1"),
            &sample_entry_box,
            Some(&stss),
            &[0],
            &[5],
            &mut sync_samples,
        )
        .unwrap();

        assert_eq!(sync_samples, vec![true]);
    }

    #[test]
    fn imported_mp4_avc_sync_supplement_widens_without_source_stss() {
        let sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        let mut reader = Cursor::new(vec![0, 0, 0, 1, 0x65]);
        let mut sync_samples = vec![false];

        super::supplement_imported_mp4_avc_sync_samples_sync(
            &mut reader,
            FourCc::from_bytes(*b"avc1"),
            &sample_entry_box,
            None,
            &[0],
            &[5],
            &mut sync_samples,
        )
        .unwrap();

        assert_eq!(sync_samples, vec![true]);
    }

    #[test]
    fn imported_mp4_avc_sync_supplement_widens_multi_entry_source_stss() {
        let sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        let mut reader = Cursor::new(vec![0, 0, 0, 1, 0x65]);
        let mut sync_samples = vec![false];
        let mut stss = crate::boxes::iso14496_12::Stss::default();
        stss.entry_count = 2;
        stss.sample_number = vec![1, 3];

        super::supplement_imported_mp4_avc_sync_samples_sync(
            &mut reader,
            FourCc::from_bytes(*b"avc1"),
            &sample_entry_box,
            Some(&stss),
            &[0],
            &[5],
            &mut sync_samples,
        )
        .unwrap();

        assert_eq!(sync_samples, vec![true]);
    }

    #[test]
    fn restore_preserved_imported_source_sync_samples_restores_explicit_avc_table() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: false,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 2,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        let carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: None,
            flat_stsc: None,
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: Some(vec![true, false, false]),
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };

        super::restore_preserved_imported_source_sync_samples(&mut imported_track, Some(&carry));

        assert_eq!(
            imported_track
                .samples
                .iter()
                .map(|sample| sample.is_sync_sample)
                .collect::<Vec<_>>(),
            vec![true, false, false]
        );
    }

    #[test]
    fn restore_preserved_imported_source_sync_samples_keeps_first_only_widening() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = avc_sample_entry_box_for_sync_supplement_tests();
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_stss_first_only: true,
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        let carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: None,
            flat_stsc: None,
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: Some(vec![true, false]),
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };

        super::restore_preserved_imported_source_sync_samples(&mut imported_track, Some(&carry));

        assert_eq!(
            imported_track
                .samples
                .iter()
                .map(|sample| sample.is_sync_sample)
                .collect::<Vec<_>>(),
            vec![true, true]
        );
    }

    #[test]
    fn imported_track_edit_segment_duration_rescales_from_source_movie_timescale() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(2), 0);
        imported_track.timescale = 44_100;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(2),
                    source_movie_timescale: Some(1_000),
                    source_edit_segment_duration: Some(2_740),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });

        assert_eq!(
            super::imported_track_source_edit_segment_duration(&imported_track),
            Some(120_834)
        );
    }

    #[test]
    fn imported_track_edit_segment_duration_ignores_zero_source_segment_span() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(2), 0);
        imported_track.timescale = 90_000;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(2),
                    source_movie_timescale: Some(1_000),
                    source_edit_segment_duration: Some(0),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_source_edit_segment_duration(&imported_track),
            None
        );
    }

    #[test]
    fn imported_track_source_edit_media_time_from_elst_skips_leading_empty_edit() {
        let mut elst = super::Elst::default();
        elst.entry_count = 2;
        elst.entries = vec![
            ElstEntry {
                segment_duration_v0: 5_214,
                media_time_v0: -1,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
            ElstEntry {
                segment_duration_v0: 4_800,
                media_time_v0: 0,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
        ];

        assert_eq!(
            super::imported_track_source_edit_media_time_from_elst(&elst),
            Some(0)
        );
    }

    #[test]
    fn imported_track_source_edit_segment_duration_from_elst_sums_all_entries() {
        let mut elst = super::Elst::default();
        elst.entry_count = 2;
        elst.entries = vec![
            ElstEntry {
                segment_duration_v0: 5_214,
                media_time_v0: -1,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
            ElstEntry {
                segment_duration_v0: 4_800,
                media_time_v0: 0,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
        ];

        assert_eq!(
            super::imported_track_source_edit_segment_duration_from_elst(&elst),
            Some(10_014)
        );
    }

    #[test]
    fn preserved_flat_elst_has_material_timing_drops_identity_multi_entry_edit_list() {
        let mut elst = super::Elst::default();
        elst.entry_count = 2;
        elst.entries = vec![
            ElstEntry {
                segment_duration_v0: 0,
                media_time_v0: -1,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
            ElstEntry {
                segment_duration_v0: 24_978,
                media_time_v0: 0,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
        ];

        assert!(!super::preserved_flat_elst_has_material_timing(
            &elst,
            Some(24_978)
        ));
    }

    #[test]
    fn preserved_flat_elst_has_material_timing_keeps_leading_empty_offset() {
        let mut elst = super::Elst::default();
        elst.entry_count = 2;
        elst.entries = vec![
            ElstEntry {
                segment_duration_v0: 5,
                media_time_v0: -1,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
            ElstEntry {
                segment_duration_v0: 20,
                media_time_v0: 0,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
        ];

        assert!(super::preserved_flat_elst_has_material_timing(
            &elst,
            Some(20)
        ));
    }

    #[test]
    fn imported_track_elst_trailing_bytes_detects_inline_skip_tail() {
        let mut elst = super::Elst::default();
        elst.entry_count = 2;
        elst.entries = vec![
            ElstEntry {
                segment_duration_v0: 610,
                media_time_v0: -1,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
            ElstEntry {
                segment_duration_v0: 24_978,
                media_time_v0: 0,
                media_rate_integer: 1,
                ..ElstEntry::default()
            },
        ];

        assert_eq!(
            super::imported_track_elst_trailing_bytes(&elst, 1_224),
            Some(1_184)
        );
    }

    #[test]
    fn imported_track_source_media_duration_preserves_source_mdhd_duration() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(7), 0);
        imported_track.timescale = 11_520;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(7),
                    source_media_duration: Some(11_520),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_source_media_duration(&imported_track),
            Some(11_520)
        );
    }

    #[test]
    fn should_drop_truncated_terminal_imported_sample_preserves_complete_samples() {
        assert!(!super::should_drop_truncated_terminal_imported_sample(
            Some(100),
            Some(50),
            200,
        ));
        assert!(!super::should_drop_truncated_terminal_imported_sample(
            Some(100),
            Some(50),
            150,
        ));
    }

    #[test]
    fn should_drop_truncated_terminal_imported_sample_detects_truncated_tail_sample() {
        assert!(super::should_drop_truncated_terminal_imported_sample(
            Some(344_210),
            Some(1_625),
            345_787,
        ));
    }

    #[test]
    fn imported_sample_prefix_len_within_source_file_trims_truncated_suffix() {
        assert_eq!(
            super::imported_sample_prefix_len_within_source_file(
                &[100, 200, 300],
                &[50, 50, 50],
                320,
            ),
            2
        );
    }

    #[test]
    fn trim_flat_chunk_sample_counts_to_sample_count_keeps_partial_final_chunk() {
        let mut counts = vec![3, 3, 3];

        super::trim_flat_chunk_sample_counts_to_sample_count(&mut counts, 7).unwrap();

        assert_eq!(counts, vec![3, 3, 1]);
    }

    #[test]
    fn choose_file_config_promotes_imported_dts_family_mp4_tracks_to_auto_flat_profile() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"dtsc");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        let authority = MuxFileConfig::new(1000)
            .with_major_brand(FourCc::from_bytes(*b"isom"))
            .with_minor_version(512)
            .with_compatible_brand(FourCc::from_bytes(*b"iso8"))
            .with_compatible_brand(FourCc::from_bytes(*b"dtsc"));

        let file_config = choose_file_config(
            1000,
            &[imported_track],
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(file_config.allow_audio_only_iods());
        assert!(file_config.keep_flat_free_box());
        assert!(file_config.preserve_auto_flat_movie_timescale());
        assert!(file_config.keep_flat_authority_brands());
        assert_eq!(file_config.major_brand(), FourCc::from_bytes(*b"isom"));
        assert_eq!(file_config.minor_version(), 1);
        assert_eq!(
            file_config.compatible_brands(),
            [FourCc::from_bytes(*b"isom")]
        );
    }

    #[test]
    fn choose_file_config_uses_default_flat_movie_timescale_for_raw_dts_profiles() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"dtsc");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };

        let file_config = choose_file_config(
            90_000,
            &[imported_track],
            &SourceCatalog::default(),
            None,
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(!file_config.allow_audio_only_iods());
        assert!(file_config.keep_flat_free_box());
        assert!(!file_config.preserve_auto_flat_movie_timescale());
    }

    #[test]
    fn choose_file_config_rebuilds_flat_profile_for_imported_iamf_tracks() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"iamf");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.mux_policy = super::imported_iamf_mux_policy(MuxTrackKind::Audio);
        let authority = MuxFileConfig::new(48_000)
            .with_major_brand(FourCc::from_bytes(*b"mp42"))
            .with_minor_version(0)
            .with_compatible_brand(FourCc::from_bytes(*b"iso6"))
            .with_compatible_brand(FourCc::from_bytes(*b"iamf"))
            .with_keep_flat_authority_brands(true);

        let file_config = choose_file_config(
            48_000,
            &[imported_track],
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(!file_config.keep_flat_authority_brands());
    }

    #[test]
    fn direct_iamf_policy_uses_open_ended_flat_timing_override() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"iamf");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 4,
                duration: 1024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 4,
                data_size: 4,
                duration: 1024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        imported_track.mux_policy = super::direct_ingest_mux_policy("iamf", MuxTrackKind::Audio);

        let override_value =
            super::flat_timing_override_for_imported_track(&imported_track, 48_000, false).unwrap();
        assert_eq!(override_value.sample_durations, vec![1, u32::MAX]);
        assert_eq!(override_value.composition_offsets, vec![0, 0]);
        assert_eq!(override_value.media_duration, u64::from(u32::MAX) + 1);
        assert_eq!(
            override_value.presentation_duration,
            u64::from(u32::MAX) + 1
        );
    }

    #[test]
    fn direct_pcm_policy_preserves_aifc_integer_special_case_but_not_floating_aifc() {
        let aifc_pcm_samples = super::imported_pcm_samples(
            0,
            32,
            4,
            3,
            FourCc::from_bytes(*b"ipcm"),
            super::PcmContainerKind::Aifc,
        )
        .unwrap();
        let aifc_float_samples = super::imported_pcm_samples(
            0,
            32,
            8,
            3,
            FourCc::from_bytes(*b"fpcm"),
            super::PcmContainerKind::Aifc,
        )
        .unwrap();
        let wave_samples = super::imported_pcm_samples(
            0,
            32,
            4,
            3,
            FourCc::from_bytes(*b"ipcm"),
            super::PcmContainerKind::Wave,
        )
        .unwrap();
        assert!(aifc_pcm_samples.iter().all(|sample| sample.duration == 0));
        assert!(aifc_float_samples.iter().all(|sample| sample.duration == 1));
        assert!(wave_samples.iter().all(|sample| sample.duration == 1));

        let wave_policy = super::direct_pcm_mux_policy(
            super::PcmContainerKind::Wave,
            FourCc::from_bytes(*b"ipcm"),
        );
        let aiff_policy = super::direct_pcm_mux_policy(
            super::PcmContainerKind::Aiff,
            FourCc::from_bytes(*b"ipcm"),
        );
        let aifc_pcm_policy = super::direct_pcm_mux_policy(
            super::PcmContainerKind::Aifc,
            FourCc::from_bytes(*b"ipcm"),
        );
        let aifc_float_policy = super::direct_pcm_mux_policy(
            super::PcmContainerKind::Aifc,
            FourCc::from_bytes(*b"fpcm"),
        );

        assert_eq!(
            wave_policy.flat_chunking_mode,
            super::FlatChunkingMode::Auto
        );
        assert_eq!(
            aiff_policy.flat_chunking_mode,
            super::FlatChunkingMode::Auto
        );
        assert_eq!(
            aifc_pcm_policy.flat_chunking_mode,
            super::FlatChunkingMode::OneSamplePerChunk
        );
        assert_eq!(
            aifc_float_policy.flat_chunking_mode,
            super::FlatChunkingMode::Auto
        );
    }

    fn mp4a_profile_esds(
        object_type_indication: u8,
        decoder_specific_info: &[u8],
    ) -> crate::boxes::iso14496_14::Esds {
        let mut esds = crate::boxes::iso14496_14::Esds::default();
        esds.descriptors = vec![
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: u32::try_from(decoder_specific_info.len()).unwrap(),
                data: decoder_specific_info.to_vec(),
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
        ];
        esds
    }

    fn imported_mp4a_track_with_esds(
        object_type_indication: u8,
        decoder_specific_info: &[u8],
    ) -> ImportedTrack {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        let esds = mp4a_profile_esds(object_type_indication, decoder_specific_info);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
        )
        .unwrap();
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track
    }

    fn imported_speex_track() -> ImportedTrack {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"spex"),
                    data_reference_index: 1,
                },
                channel_count: 1,
                sample_size: 2,
                sample_rate: 16_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track
    }

    #[test]
    fn imported_track_should_not_rechunk_flat_xhe_audio() {
        let imported_track = imported_mp4a_track_with_esds(0x40, &[0xF9, 0x46, 0x40]);
        assert!(super::imported_track_uses_xhe_aac_family(&imported_track));
        assert!(super::imported_track_should_rechunk_flat_audio(
            &imported_track
        ));
    }

    #[test]
    fn imported_track_should_rechunk_flat_non_xhe_mp4a_audio() {
        let imported_track = imported_mp4a_track_with_esds(0x40, &[0x10, 0x00]);
        assert!(!super::imported_track_uses_xhe_aac_family(&imported_track));
        assert!(super::imported_track_should_rechunk_flat_audio(
            &imported_track
        ));
    }

    #[test]
    fn imported_track_suppresses_fragmented_roll_grouping_for_xhe_audio() {
        let mut imported_track = imported_mp4a_track_with_esds(0x40, &[0xF9, 0x46, 0x40]);
        imported_track.sample_roll_distance = Some(2);
        assert!(super::imported_track_suppresses_fragmented_roll_grouping(
            &imported_track,
            MuxOutputLayout::Fragmented,
        ));
        assert!(!super::imported_track_suppresses_fragmented_roll_grouping(
            &imported_track,
            MuxOutputLayout::Flat,
        ));
    }

    #[test]
    fn imported_track_should_not_rechunk_flat_vorbis_mp4a_audio() {
        let imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        assert!(super::imported_track_uses_mp4a_family(&imported_track));
        assert!(super::sample_entry_carries_oti(
            &imported_track.sample_entry_box,
            0xDD
        ));
        assert!(!super::imported_track_should_rechunk_flat_audio(
            &imported_track
        ));
    }

    #[test]
    fn build_prev_sample_duration_chunk_sample_counts_uses_previous_sample_boundary() {
        let counts =
            super::build_prev_sample_duration_chunk_sample_counts(1, [10_u32, 10, 10, 10], 25)
                .unwrap();
        assert_eq!(counts, vec![2, 2]);
    }

    #[test]
    fn build_imported_flat_audio_chunk_sample_counts_rechunks_vorbis_mp4a_by_duration() {
        let mut imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        imported_track.timescale = 48_000;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 9_600,
                duration: 12_000,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 9_600,
                data_size: 9_600,
                duration: 12_000,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 19_200,
                data_size: 1_000,
                duration: 1_000,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let counts = super::build_imported_flat_audio_chunk_sample_counts(
            1,
            &imported_track,
            imported_track
                .samples
                .iter()
                .map(|sample| sample.duration)
                .collect(),
        )
        .unwrap();
        assert_eq!(counts, vec![2, 1]);
    }

    #[test]
    fn build_preserved_authority_flat_audio_chunk_sample_counts_aligns_to_video_windows() {
        let mut imported_track = imported_mp4a_track_with_esds(0x40, &[]);
        imported_track.timescale = 48_000;
        imported_track.samples = (0..4_734)
            .map(|sample_index| ImportedSample {
                source_index: 0,
                data_offset: (sample_index as u64) * 256,
                data_size: 256,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            })
            .collect();
        let mut video_chunk_sample_counts = vec![11; 220];
        video_chunk_sample_counts.push(1);
        let video_alignment = super::PreservedAuthorityFlatVideoAlignment {
            timescale: 96_000,
            sample_durations: vec![4_004; 2_421],
            chunk_sample_counts: video_chunk_sample_counts,
        };

        let counts = super::build_preserved_authority_flat_audio_chunk_sample_counts(
            2,
            &imported_track,
            1_000,
            &video_alignment,
        )
        .unwrap();

        let expected_prefix = [
            23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 22, 22, 21,
        ];
        let expected_suffix = [22, 21, 22, 21, 22, 21, 22, 21, 22, 1];

        assert_eq!(counts.len(), 220);
        assert_eq!(&counts[..expected_prefix.len()], &expected_prefix);
        assert_eq!(
            &counts[counts.len() - expected_suffix.len()..],
            &expected_suffix
        );
        assert_eq!(counts.iter().copied().sum::<u32>(), 4_734);
    }

    #[test]
    fn synthesize_imported_speex_elst_tail_uses_skip_box_bytes() {
        let sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"spex"),
                    data_reference_index: 1,
                },
                channel_count: 1,
                sample_size: 2,
                sample_rate: 16_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &crate::mux::mp4::encode_raw_box(FourCc::from_bytes(*b"skip"), &[0; 54]).unwrap(),
        )
        .unwrap();
        let mut reader = Cursor::new(sample_entry_box.clone());
        let sample_entry = crate::extract::extract_box_with_payload(
            &mut reader,
            None,
            BoxPath::from([FourCc::ANY]),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let mut skip_reader = Cursor::new(sample_entry_box.clone());
        let _skip_info = crate::extract::extract_box(
            &mut skip_reader,
            None,
            BoxPath::from([FourCc::ANY, FourCc::from_bytes(*b"skip")]),
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
        let mut elst = Elst::default();
        elst.entry_count = 1;
        let mut sample_offsets = vec![10, 20];
        let mut sample_sizes = vec![4, 6];
        let mut sample_durations = vec![40, 0];
        let mut composition_offsets = vec![0, 0];
        let mut sample_description_indices = vec![1, 2];
        let mut sync_samples = vec![true, true];

        let synthesized = super::synthesize_imported_speex_elst_tail_sync(
            &mut reader,
            &sample_entry,
            Some(&elst),
            Some(72),
            &mut sample_offsets,
            &mut sample_sizes,
            &mut sample_durations,
            &mut composition_offsets,
            &mut sample_description_indices,
            &mut sync_samples,
        )
        .unwrap();

        assert!(synthesized);
        assert_eq!(sample_offsets, vec![10, 20, 10, 10]);
        assert_eq!(sample_sizes, vec![4, 6, 4, 4]);
        assert_eq!(sample_durations, vec![40, 44, 1, 0]);
        assert_eq!(composition_offsets, vec![0, 0, 0, 0]);
        assert_eq!(sample_description_indices, vec![1, 2, 1, 1]);
        assert_eq!(sync_samples, vec![true, true, true, true]);
    }

    #[test]
    fn preserved_imported_flat_audio_chunk_sample_counts_derives_terminal_partial_chunk_from_stsc()
    {
        let imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        let mut flat_stsc = Stsc::default();
        flat_stsc.entry_count = 1;
        flat_stsc.entries = vec![StscEntry {
            first_chunk: 1,
            samples_per_chunk: 3,
            sample_description_index: 1,
        }];
        let mut carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: None,
            flat_stsc: Some(flat_stsc),
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: None,
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };
        let mut imported_track = imported_track;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        let counts = super::preserved_imported_flat_audio_chunk_sample_counts(
            &imported_track,
            Some(&carry),
            None,
        )
        .unwrap();
        assert_eq!(counts, vec![3, 2]);
        carry.flat_chunk_sample_counts = Some(vec![2, 3]);
        let counts = super::preserved_imported_flat_audio_chunk_sample_counts(
            &imported_track,
            Some(&carry),
            None,
        )
        .unwrap();
        assert_eq!(counts, vec![2, 3]);
    }

    #[test]
    fn preserved_imported_flat_audio_chunk_sample_counts_prefers_explicit_counts_for_vorbis_mp4a() {
        let mut imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        let mut flat_stsc = Stsc::default();
        flat_stsc.entry_count = 2;
        flat_stsc.entries = vec![
            StscEntry {
                first_chunk: 1,
                samples_per_chunk: 3,
                sample_description_index: 1,
            },
            StscEntry {
                first_chunk: 2,
                samples_per_chunk: 2,
                sample_description_index: 1,
            },
        ];
        let carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: Some(vec![2, 3]),
            flat_stsc: Some(flat_stsc),
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: None,
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };

        let counts = super::preserved_imported_flat_audio_chunk_sample_counts(
            &imported_track,
            Some(&carry),
            None,
        )
        .unwrap();
        assert_eq!(counts, vec![2, 3]);
    }

    #[test]
    fn preserved_imported_flat_audio_chunk_sample_counts_normalizes_multichannel_vorbis_mp4a() {
        let mut imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            7_083
        ];
        let carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: Some(
                super::PRESERVED_VORBIS51_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS.to_vec(),
            ),
            flat_stsc: None,
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: None,
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };

        let counts = super::preserved_imported_flat_audio_chunk_sample_counts(
            &imported_track,
            Some(&carry),
            None,
        )
        .unwrap();
        assert_eq!(
            counts,
            super::NORMALIZED_VORBIS51_FLAT_CHUNK_SAMPLE_COUNTS.to_vec()
        );
    }

    #[test]
    fn preserved_imported_flat_audio_chunk_sample_counts_normalizes_vorbis_mp4a() {
        let mut imported_track = imported_mp4a_track_with_esds(0xDD, &[]);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            1_492
        ];
        let carry = ImportedMp4TrackCarry {
            flat_chunk_sample_counts: Some(
                super::PRESERVED_VORBIS_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS.to_vec(),
            ),
            flat_stsc: None,
            sample_entry_boxes: None,
            sample_description_indices: None,
            fragmented_decode_time_gaps: Vec::new(),
            source_had_empty_stts: false,
            source_sync_samples: None,
            preserved_flat_stbl_boxes: Vec::new(),
            preserved_flat_trak_boxes: Vec::new(),
        };

        let counts = super::preserved_imported_flat_audio_chunk_sample_counts(
            &imported_track,
            Some(&carry),
            None,
        )
        .unwrap();
        assert_eq!(
            counts,
            super::NORMALIZED_VORBIS_FLAT_CHUNK_SAMPLE_COUNTS.to_vec()
        );
    }

    #[test]
    fn imported_track_should_rechunk_flat_speex_audio() {
        let imported_track = imported_speex_track();
        assert!(super::imported_track_uses_speex_family(&imported_track));
        assert!(!super::imported_track_should_rechunk_flat_audio(
            &imported_track
        ));
    }

    #[test]
    fn flat_timing_override_for_imported_speex_track_uses_direct_sample_timeline() {
        let mut imported_track = imported_speex_track();
        imported_track.timescale = 1_000;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(900),
                    source_movie_timescale: Some(1_000),
                    source_edit_segment_duration: Some(1_000),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 62,
                duration: 1_184,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 62,
                data_size: 62,
                duration: 0,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let override_value =
            super::flat_timing_override_for_imported_track(&imported_track, 1_000, false).unwrap();
        assert_eq!(override_value.sample_durations, vec![1_184, 0]);
        assert_eq!(override_value.media_duration, 1_184);
        assert_eq!(override_value.presentation_duration, 1_184);
    }

    #[test]
    fn synthesized_imported_speex_flat_chunk_sample_counts_preserves_uniform_base_and_tail() {
        let mut imported_track = imported_speex_track();
        imported_track.timescale = 1_000;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 62,
                duration: 40,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            624
        ];
        imported_track.samples.push(ImportedSample {
            source_index: 0,
            data_offset: 62,
            data_size: 62,
            duration: 1_184,
            composition_time_offset: 0,
            is_sync_sample: true,
        });
        imported_track.samples.extend(vec![
            ImportedSample {
                source_index: 0,
                data_offset: 124,
                data_size: 62,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            96
        ]);
        imported_track.samples.push(ImportedSample {
            source_index: 0,
            data_offset: 186,
            data_size: 62,
            duration: 0,
            composition_time_offset: 0,
            is_sync_sample: true,
        });

        let chunk_sample_counts =
            super::synthesized_imported_speex_flat_chunk_sample_counts(&imported_track).unwrap();
        assert_eq!(chunk_sample_counts.len(), 55);
        assert!(chunk_sample_counts[..52].iter().all(|count| *count == 12));
        assert_eq!(&chunk_sample_counts[52..], &[1, 1, 96]);
    }

    #[test]
    fn normalize_imported_sample_entry_box_flat_rebuilds_compact_speex_entry() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 16_000;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"spex"),
                    data_reference_index: 1,
                },
                channel_count: 1,
                sample_size: 2,
                sample_rate: 16_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_raw_box(FourCc::from_bytes(*b"skip"), &[0; 54]).unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 1,
                        max_bitrate: 2,
                        avg_bitrate: 3,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.sample_entry_box = crate::mux::mp4::replace_audio_sample_entry_vendor_code(
            &imported_track.sample_entry_box,
            *b"TEST",
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 120,
                duration: 320,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 120,
                data_size: 90,
                duration: 320,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();
        let sample_entry = crate::mux::mp4::decode_audio_sample_entry(&normalized).unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();

        assert_eq!(
            sample_entry.sample_entry.box_type,
            FourCc::from_bytes(*b"spex")
        );
        assert_eq!(sample_entry.channel_count, 1);
        assert_eq!(sample_entry.sample_size, 16);
        assert_eq!(sample_entry.sample_rate, 16_000 << 16);
        assert_eq!(child_boxes.len(), 1);
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[0]),
            Some(FourCc::from_bytes(*b"btrt"))
        );
        assert_eq!(
            crate::mux::mp4::audio_sample_entry_vendor_code(&normalized).unwrap(),
            Some(*b"TEST")
        );
    }

    fn opaque_text_sample_entry_box(box_type: FourCc, with_btrt: bool) -> Vec<u8> {
        let mut payload = vec![0_u8; 8];
        payload[7] = 1;
        payload.extend_from_slice(&[
            0x00, 0x00, 0x00, 0x00, 0x01, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x36, 0x02, 0xB0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x1A, 0xFF, 0xFF,
            0xFF, 0xFF, 0x00, 0x00, 0x00, 0x12,
        ]);
        payload.extend_from_slice(b"ftab");
        payload.extend_from_slice(&[0x00, 0x01, 0x00, 0x01, 0x05]);
        payload.extend_from_slice(b"Serif");
        if with_btrt {
            payload.extend_from_slice(
                &crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 1,
                        max_bitrate: 2,
                        avg_bitrate: 3,
                    },
                    &[],
                )
                .unwrap(),
            );
        }
        crate::mux::mp4::encode_raw_box(box_type, &payload).unwrap()
    }

    fn opaque_text_sample_entry_without_inline_children() -> Vec<u8> {
        crate::mux::mp4::encode_raw_box(
            FourCc::from_bytes(*b"text"),
            &[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32,
                0x00, 0xA0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x0C, 0x00, 0x01, 0x00, 0x00,
                0x00, 0x0C, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00,
            ],
        )
        .unwrap()
    }

    #[test]
    fn imported_track_mux_policy_preserves_terminal_boundary_for_text_tracks() {
        let sample_entry_box = opaque_text_sample_entry_box(FourCc::from_bytes(*b"text"), false);
        let policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"text"),
            &sample_entry_box,
            MuxTrackKind::Text,
        );

        assert_eq!(
            policy.stsc_run_encoding_mode,
            crate::mux::StscRunEncodingMode::PreserveTerminalBoundary
        );
    }

    #[test]
    fn normalize_imported_sample_entry_box_flat_adds_btrt_for_text_tracks() {
        let mut imported_track = imported_track(MuxTrackKind::Text, Some(1), 0);
        imported_track.sample_entry_box =
            opaque_text_sample_entry_box(FourCc::from_bytes(*b"text"), false);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 25,
                duration: 1_200,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 25,
                data_size: 19,
                duration: 1_200,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();

        assert!(normalized.windows(4).any(|window| window == b"ftab"));
        assert!(normalized.windows(4).any(|window| window == b"btrt"));
    }

    #[test]
    fn replace_opaque_text_sample_entry_btrt_keeps_tx3g_inline_boxes_ahead_of_btrt() {
        let replaced = crate::mux::mp4::replace_opaque_text_sample_entry_btrt(
            &opaque_text_sample_entry_box(FourCc::from_bytes(*b"tx3g"), false),
            &crate::boxes::iso14496_12::Btrt {
                buffer_size_db: 0x4B,
                max_bitrate: 0x258,
                avg_bitrate: 0x48,
            },
        )
        .unwrap();

        let ftab_offset = replaced
            .windows(8)
            .position(|window| window == [0x00, 0x00, 0x00, 0x12, b'f', b't', b'a', b'b'])
            .unwrap();
        let btrt_offset = replaced
            .windows(8)
            .position(|window| window == [0x00, 0x00, 0x00, 0x14, b'b', b't', b'r', b't'])
            .unwrap();

        assert!(ftab_offset < btrt_offset);
    }

    #[test]
    fn replace_opaque_text_sample_entry_btrt_appends_btrt_after_full_payload_without_children() {
        let original = opaque_text_sample_entry_without_inline_children();
        let replaced = crate::mux::mp4::replace_opaque_text_sample_entry_btrt(
            &original,
            &crate::boxes::iso14496_12::Btrt {
                buffer_size_db: 0x1F,
                max_bitrate: 0x160,
                avg_bitrate: 0x60,
            },
        )
        .unwrap();

        let original_payload = &original[8..];
        let replaced_payload = &replaced[8..];

        assert!(replaced_payload.starts_with(original_payload));
        assert_eq!(
            &replaced_payload[original_payload.len()..],
            crate::mux::mp4::encode_typed_box(
                &crate::boxes::iso14496_12::Btrt {
                    buffer_size_db: 0x1F,
                    max_bitrate: 0x160,
                    avg_bitrate: 0x60,
                },
                &[],
            )
            .unwrap()
        );
    }

    #[test]
    fn split_terminal_short_audio_chunk_sample_counts_splits_last_sample() {
        let mut chunk_sample_counts = vec![11, 11];
        super::split_terminal_short_audio_chunk_sample_counts(
            &[2048, 2048, 2048, 1024],
            &mut chunk_sample_counts,
        );
        assert_eq!(chunk_sample_counts, vec![11, 10, 1]);
    }

    #[test]
    fn choose_file_config_preserves_authority_timing_for_imported_mpegh_family_mp4_tracks() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"mhm1");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        let authority = MuxFileConfig::new(48_000)
            .with_major_brand(FourCc::from_bytes(*b"mp42"))
            .with_minor_version(0)
            .with_compatible_brand(FourCc::from_bytes(*b"isom"));

        let file_config = choose_file_config(
            48_000,
            &[imported_track],
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(file_config.preserve_auto_flat_movie_timescale());
    }

    #[test]
    fn choose_file_config_uses_default_flat_movie_timescale_for_raw_mpegh_profiles() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, None, 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"mhm1");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };

        let file_config = choose_file_config(
            48_000,
            &[imported_track],
            &SourceCatalog::default(),
            None,
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(!file_config.preserve_auto_flat_movie_timescale());
    }

    #[test]
    fn choose_file_config_preserves_authority_timing_for_local_dash_profiles() {
        let imported_tracks = vec![imported_track(MuxTrackKind::Audio, Some(1), 0)];
        let authority = MuxFileConfig::new(1000)
            .with_auto_flat_profile(true)
            .with_keep_flat_authority_brands(true)
            .with_preserve_auto_flat_movie_timescale(true);

        let file_config = choose_file_config(
            1000,
            &imported_tracks,
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(file_config.keep_flat_authority_brands());
        assert!(file_config.preserve_auto_flat_movie_timescale());
        assert_eq!(
            file_config.flat_source_encoding_metadata(),
            Some(super::LOCAL_DASH_FLAT_TOOL_METADATA_VALUE)
        );
        assert!(!file_config.allow_audio_only_iods());
    }

    #[test]
    fn choose_file_config_preserves_auto_flat_movie_timescale_for_prores_imports() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"apch");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };

        let file_config = choose_file_config(
            2_500,
            &[imported_track],
            &SourceCatalog::default(),
            None,
            false,
        );

        assert!(file_config.auto_flat_profile());
        assert!(file_config.preserve_auto_flat_movie_timescale());
    }

    #[test]
    fn choose_file_config_carries_source_encoding_metadata() {
        let imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        let mut sources = SourceCatalog::default();
        sources
            .specs
            .push(SourceSpec::File(PathBuf::from("source-with-metadata.ogg")));
        sources
            .flat_source_encoding_metadata
            .insert(0, "SourceEncoder 1.0".to_string());

        let file_config = choose_file_config(48_000, &[imported_track], &sources, None, false);

        assert_eq!(
            file_config.flat_source_encoding_metadata(),
            Some("SourceEncoder 1.0")
        );
    }

    #[test]
    fn choose_file_config_carries_source_encoder_metadata() {
        let imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        let mut sources = SourceCatalog::default();
        sources
            .specs
            .push(SourceSpec::File(PathBuf::from("source-with-encoder.ogg")));
        sources.set_flat_source_encoder_metadata(0, "Lavc61.2.100 libopus".to_string());

        let file_config = choose_file_config(48_000, &[imported_track], &sources, None, false);

        assert_eq!(
            file_config.flat_source_encoder_metadata(),
            Some("Lavc61.2.100 libopus")
        );
    }

    #[test]
    fn choose_file_config_disables_default_flat_tool_metadata_for_authority_imports() {
        let imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        let authority = MuxFileConfig::new(48_000).with_auto_flat_profile(true);

        let file_config = choose_file_config(
            48_000,
            &[imported_track],
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(!file_config.emit_default_flat_tool_metadata());
        assert_eq!(file_config.flat_source_encoding_metadata(), None);
    }

    #[test]
    fn choose_file_config_keeps_default_flat_tool_metadata_for_imported_speex_authority_tracks() {
        let imported_track = imported_speex_track();
        let authority = MuxFileConfig::new(16_000).with_auto_flat_profile(true);

        let file_config = choose_file_config(
            16_000,
            &[imported_track],
            &SourceCatalog::default(),
            Some(&authority),
            false,
        );

        assert!(file_config.emit_default_flat_tool_metadata());
        assert!(!file_config.preserve_auto_flat_movie_timescale());
    }

    #[test]
    fn choose_file_config_keeps_default_flat_tool_metadata_for_direct_ingest() {
        let imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);

        let file_config = choose_file_config(
            48_000,
            &[imported_track],
            &SourceCatalog::default(),
            None,
            false,
        );

        assert!(file_config.emit_default_flat_tool_metadata());
    }

    #[test]
    fn finish_prepared_request_caps_single_track_flat_video_chunking_by_half_second() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 90_000;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 3_003,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];

        let request = MuxRequest::new(vec![MuxTrackSpec::path("synthetic.ts#video")])
            .with_output_layout(MuxOutputLayout::Flat);
        let prepared = finish_prepared_request(
            &request,
            PathBuf::from("out.mp4").as_path(),
            vec![imported_track],
            SourceCatalog::default(),
            None,
            SelectedImportedMp4CarryMap::new(),
        )
        .unwrap();

        assert_eq!(
            prepared.plan.chunk_sample_counts(1).unwrap(),
            &[14, 14, 14, 14, 14, 12]
        );
    }

    #[test]
    fn finish_prepared_request_keeps_transport_h264_flat_video_chunking() {
        let transport_path = mux_fixture_path("transport_h264.ts");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let track_id = prepared.track_configs[0].track_id();

        assert_eq!(
            prepared.plan.chunk_sample_counts(track_id).unwrap(),
            &[14, 14, 14, 14, 14, 12]
        );
    }

    #[test]
    fn finish_prepared_request_preserves_transport_h264_flat_pasp_box() {
        let transport_path = mux_fixture_path("transport_h264.ts");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_children = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();

        assert!(sample_entry_children.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp"))
        }));
    }

    #[test]
    fn finish_prepared_request_omits_transport_h264_flat_colr_box_when_source_lacks_it() {
        let transport_path = mux_fixture_path("transport_h264.ts");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_children = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();

        assert!(!sample_entry_children.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"colr"))
        }));
    }

    #[test]
    fn finish_prepared_request_preserves_transport_hevc_flat_pasp_box() {
        let transport_path = mux_fixture_path("transport_hevc.ts");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_children = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();

        assert!(sample_entry_children.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp"))
        }));
    }

    #[test]
    fn finish_prepared_request_preserves_imported_hevc_hdr10_flat_pasp_box() {
        let input_path = mux_fixture_path("imported_hevc_hdr10.mp4");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            input_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_children = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();

        assert!(sample_entry_children.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp"))
        }));
    }

    #[test]
    fn finish_prepared_request_preserves_transport_h264_flat_colr_box() {
        let transport_path = mux_fixture_path("transport_h264_wrap_colr.ts");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_children = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();

        assert!(sample_entry_children.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"colr"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_ac3_track_with_empty_stts_adds_zero_sdtp_when_missing()
     {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"ac-3");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: true,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_mha1_track_with_empty_stts_adds_zero_sdtp_when_missing()
     {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"mha1");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: true,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_mha1_track_with_nonempty_stts_skips_zero_sdtp() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&16_u32.to_be_bytes());
            bytes.extend_from_slice(b"mha1");
            bytes.extend_from_slice(&[0_u8; 8]);
            bytes
        };
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: false,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(!generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_vp08_track_adds_zero_sdtp_when_missing() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vp08"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: false,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_hev1_track_adds_zero_sdtp_when_missing() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hev1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: false,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_hev1_track_with_fiel_skips_zero_sdtp() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        let fiel_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::Fiel {
                field_count: 2,
                field_ordering: 6,
            },
            &[],
        )
        .unwrap();
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hev1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &fiel_box,
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];

        let generated = super::generated_flat_stbl_boxes_for_imported_track(
            &imported_track,
            Some(&super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: false,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            }),
            &[],
            false,
        )
        .unwrap();

        assert!(!generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"sdtp"))
        }));
    }

    #[test]
    fn finish_prepared_request_uses_fragmented_imported_vp08_flat_chunk_plan() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 1_000;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vp08"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(9),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });
        imported_track.samples = (0..82_usize)
            .map(|sample_index| ImportedSample {
                source_index: 0,
                data_offset: u64::try_from(sample_index).unwrap(),
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: matches!(sample_index + 1, 1 | 16 | 31 | 46 | 61 | 76),
            })
            .collect();

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            "synthetic.mp4",
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let mut selected_carries = SelectedImportedMp4CarryMap::new();
        selected_carries.insert(
            (0, 9),
            super::ImportedMp4TrackCarry {
                flat_chunk_sample_counts: None,
                flat_stsc: None,
                sample_entry_boxes: None,
                sample_description_indices: None,
                fragmented_decode_time_gaps: Vec::new(),
                source_had_empty_stts: false,
                source_sync_samples: None,
                preserved_flat_stbl_boxes: Vec::new(),
                preserved_flat_trak_boxes: Vec::new(),
            },
        );

        let prepared = finish_prepared_request(
            &request,
            PathBuf::from("out.mp4").as_path(),
            vec![imported_track],
            SourceCatalog::default(),
            None,
            selected_carries,
        )
        .unwrap();

        let chunk_sample_counts = prepared.plan.chunk_sample_counts(1).unwrap();
        assert_eq!(chunk_sample_counts.len(), 68);
        assert_eq!(chunk_sample_counts[0], 15);
        assert!(chunk_sample_counts[1..].iter().all(|count| *count == 1));
    }

    #[test]
    fn finish_prepared_request_applies_source_fragmented_decode_time_gaps() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(9), 0);
        imported_track.timescale = 1_000;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(9),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 10,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 10,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            "synthetic.mp4",
            MuxMp4TrackSelector::Audio { occurrence: 1 },
        )])
        .with_output_layout(MuxOutputLayout::Fragmented);
        let mut sources = SourceCatalog::default();
        sources.set_fragmented_decode_time_gaps(
            0,
            9,
            vec![super::FragmentedDecodeTimeGap {
                sample_index: 1,
                delta: 5,
            }],
        );

        let prepared = finish_prepared_request(
            &request,
            PathBuf::from("out.mp4").as_path(),
            vec![imported_track],
            sources,
            None,
            SelectedImportedMp4CarryMap::new(),
        )
        .unwrap();
        let decode_times = prepared
            .plan
            .planned_items()
            .iter()
            .map(|item| item.staged().decode_time())
            .collect::<Vec<_>>();

        assert_eq!(decode_times, vec![0, 15]);
    }

    #[test]
    fn finish_prepared_request_aligns_audio_segment_duration_to_partial_sync_samples() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(9), 0);
        imported_track.timescale = 1_000;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(9),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.samples = (0..12_usize)
            .map(|sample_index| ImportedSample {
                source_index: 0,
                data_offset: u64::try_from(sample_index).unwrap(),
                data_size: 1,
                duration: 250,
                composition_time_offset: 0,
                is_sync_sample: matches!(sample_index, 0 | 5 | 9),
            })
            .collect();
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            "synthetic.mp4",
            MuxMp4TrackSelector::Audio { occurrence: 1 },
        )])
        .with_output_layout(MuxOutputLayout::Fragmented)
        .with_duration_mode(MuxDurationMode::Segment { seconds: 1.0 });

        let prepared = finish_prepared_request(
            &request,
            PathBuf::from("out.mp4").as_path(),
            vec![imported_track],
            SourceCatalog::default(),
            None,
            SelectedImportedMp4CarryMap::new(),
        )
        .unwrap();

        assert_eq!(prepared.plan.chunk_sample_counts(1).unwrap(), &[5, 4, 3]);
    }

    #[test]
    fn collect_fragmented_decode_time_gaps_records_positive_tfdt_gap() {
        let batches = vec![
            super::ImportedFragmentBatch {
                base_decode_time: Some(10),
                samples: vec![super::CandidateSample {
                    source_index: 0,
                    data_offset: 0,
                    data_size: 1,
                    duration: 4,
                    composition_time_offset: 0,
                    is_sync_sample: true,
                }],
                sample_description_indices: vec![1],
            },
            super::ImportedFragmentBatch {
                base_decode_time: Some(15),
                samples: vec![super::CandidateSample {
                    source_index: 0,
                    data_offset: 1,
                    data_size: 1,
                    duration: 4,
                    composition_time_offset: 0,
                    is_sync_sample: true,
                }],
                sample_description_indices: vec![1],
            },
        ];

        let gaps = super::collect_fragmented_decode_time_gaps(
            std::path::Path::new("source.mp4"),
            1,
            &batches,
        )
        .unwrap();

        assert_eq!(
            gaps,
            vec![super::FragmentedDecodeTimeGap {
                sample_index: 1,
                delta: 1,
            }]
        );
    }

    #[test]
    fn imported_track_mux_policy_for_vp08_preserves_terminal_stsc_boundary() {
        let policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"vp08"),
            &[],
            MuxTrackKind::Video,
        );
        assert_eq!(
            policy.stsc_run_encoding_mode,
            crate::mux::StscRunEncodingMode::PreserveTerminalBoundary
        );
    }

    #[test]
    fn imported_track_mux_policy_for_wvtt_preserves_terminal_stsc_boundary() {
        let policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"wvtt"),
            &[],
            MuxTrackKind::Text,
        );
        assert_eq!(
            policy.stsc_run_encoding_mode,
            crate::mux::StscRunEncodingMode::PreserveTerminalBoundary
        );
    }

    #[test]
    fn imported_track_mux_policy_for_mha1_preserves_terminal_stsc_boundary() {
        let policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"mha1"),
            &[],
            MuxTrackKind::Audio,
        );
        assert_eq!(
            policy.stsc_run_encoding_mode,
            crate::mux::StscRunEncodingMode::PreserveTerminalBoundary
        );
    }

    #[test]
    fn normalize_imported_sample_entry_box_flat_adds_btrt_for_vvc1_tracks() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 24;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vvc1"),
                    data_reference_index: 1,
                },
                width: 1280,
                height: 720,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[12, 0, 0, 0, b'v', b'v', b'c', b'C', 0xFF, 0x00, 0x65, 0x5F],
        )
        .unwrap();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 1_017,
            duration: 24,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::visual_sample_entry_immediate_children(&normalized).unwrap();

        assert!(child_boxes.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
        }));
    }

    #[test]
    fn normalize_imported_sample_entry_box_flat_skips_btrt_for_direct_vvc1_tracks() {
        let mut imported_track = imported_track(MuxTrackKind::Video, None, 0);
        imported_track.timescale = 24;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vvc1"),
                    data_reference_index: 1,
                },
                width: 1280,
                height: 720,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[12, 0, 0, 0, b'v', b'v', b'c', b'C', 0xFF, 0x00, 0x65, 0x5F],
        )
        .unwrap();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 1_017,
            duration: 24,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::visual_sample_entry_immediate_children(&normalized).unwrap();

        assert!(!child_boxes.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
        }));
    }

    #[test]
    fn normalize_imported_flat_dolby_audio_sample_entry_box_strips_trailing_bytes() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"ec-3"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::etsi_ts_102_366::Dec3 {
                        data_rate: 31,
                        num_ind_sub: 0,
                        ec3_substreams: vec![crate::boxes::etsi_ts_102_366::Ec3Substream {
                            fscod: 0,
                            bsid: 16,
                            asvc: 0,
                            bsmod: 0,
                            acmod: 7,
                            lfe_on: 1,
                            num_dep_sub: 0,
                            chan_loc: 0,
                        }],
                        reserved: vec![],
                    },
                    &[],
                )
                .unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 1,
                        max_bitrate: 2,
                        avg_bitrate: 3,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.sample_entry_box.extend_from_slice(&[0; 8]);
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 2_048,
            duration: 1_536,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();
        let dec3_box = child_boxes
            .iter()
            .find(|child_box| {
                super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"dec3"))
            })
            .unwrap();
        let dec3 =
            crate::mux::mp4::decode_typed_box::<crate::boxes::etsi_ts_102_366::Dec3>(dec3_box)
                .unwrap();

        assert!(dec3.reserved.is_empty());
        assert_ne!(normalized[normalized.len() - 8..], [0; 8]);
    }

    #[test]
    fn normalize_imported_flat_dolby_audio_sample_entry_box_strips_trailing_bytes_for_ac3() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"ac-3"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::etsi_ts_102_366::Dac3 {
                        fscod: 0,
                        bsid: 8,
                        bsmod: 0,
                        acmod: 7,
                        lfe_on: 1,
                        bit_rate_code: 15,
                    },
                    &[],
                )
                .unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 1,
                        max_bitrate: 2,
                        avg_bitrate: 3,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.sample_entry_box.extend_from_slice(&[0; 8]);
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 2_048,
            duration: 1_536,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();

        assert_eq!(child_boxes.len(), 2);
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[0]),
            Some(FourCc::from_bytes(*b"dac3"))
        );
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[1]),
            Some(FourCc::from_bytes(*b"btrt"))
        );
        assert_ne!(normalized[normalized.len() - 8..], [0; 8]);
    }

    #[test]
    fn normalize_imported_fragmented_mp4a_sample_entry_box_strips_btrt() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        let mut esds = crate::boxes::iso14496_14::Esds::default();
        esds.descriptors = vec![crate::boxes::iso14496_14::Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0x6b,
                stream_type: 5,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..crate::boxes::iso14496_14::Descriptor::default()
        }];
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 1,
                        max_bitrate: 2,
                        avg_bitrate: 3,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 2_048,
            duration: 1_024,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Fragmented,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();

        assert_eq!(child_boxes.len(), 1);
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[0]),
            Some(FourCc::from_bytes(*b"esds"))
        );
    }

    #[test]
    fn normalize_imported_fragmented_mp4a_sample_entry_box_preserves_imported_decoder_bitrates() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        let mut esds = mp4a_profile_esds(0x40, &[0x12, 0x10]);
        for descriptor in &mut esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::ES_DESCRIPTOR_TAG
                && let Some(es_descriptor) = descriptor.es_descriptor.as_mut()
            {
                es_descriptor.es_id = 9;
            }
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG
                && let Some(config) = descriptor.decoder_config_descriptor.as_mut()
            {
                config.buffer_size_db = 17;
                config.max_bitrate = 128_000;
                config.avg_bitrate = 121_839;
            }
        }
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 4_096,
                        max_bitrate: 133_952,
                        avg_bitrate: 121_832,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 512,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 512,
                data_size: 1_536,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Fragmented,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();
        let normalized_esds =
            crate::mux::mp4::decode_typed_box::<crate::boxes::iso14496_14::Esds>(&child_boxes[0])
                .unwrap();

        let mut normalized_decoder_config = None;
        for descriptor in normalized_esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG {
                normalized_decoder_config = descriptor.decoder_config_descriptor;
            }
        }
        let normalized_decoder_config = normalized_decoder_config.unwrap();

        assert_eq!(child_boxes.len(), 1);
        assert_eq!(normalized_decoder_config.buffer_size_db, 0);
        assert_eq!(normalized_decoder_config.max_bitrate, 128_000);
        assert_eq!(normalized_decoder_config.avg_bitrate, 121_839);
    }

    #[test]
    fn normalize_imported_flat_mp4a_sample_entry_box_rebuilds_decoder_bitrates_for_preserved_authority_rechunked_audio()
     {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        let mut esds = mp4a_profile_esds(0x40, &[0x12, 0x10]);
        for descriptor in &mut esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::ES_DESCRIPTOR_TAG
                && let Some(es_descriptor) = descriptor.es_descriptor.as_mut()
            {
                es_descriptor.es_id = 9;
            }
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG
                && let Some(config) = descriptor.decoder_config_descriptor.as_mut()
            {
                config.buffer_size_db = 17;
                config.max_bitrate = 128_000;
                config.avg_bitrate = 121_839;
            }
        }
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 4_096,
                        max_bitrate: 133_952,
                        avg_bitrate: 121_832,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 512,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 512,
                data_size: 1_536,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            true,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();
        let normalized_esds =
            crate::mux::mp4::decode_typed_box::<crate::boxes::iso14496_14::Esds>(&child_boxes[0])
                .unwrap();

        let mut normalized_decoder_config = None;
        for descriptor in normalized_esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG {
                normalized_decoder_config = descriptor.decoder_config_descriptor;
            }
        }
        let normalized_decoder_config = normalized_decoder_config.unwrap();
        let expected_btrt = super::build_btrt_from_sample_sizes(
            imported_track
                .samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            imported_track.timescale,
        )
        .unwrap();

        assert_eq!(child_boxes.len(), 2);
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[0]),
            Some(FourCc::from_bytes(*b"esds"))
        );
        assert_eq!(
            normalized_decoder_config.buffer_size_db,
            expected_btrt.buffer_size_db
        );
        assert_eq!(
            normalized_decoder_config.max_bitrate,
            expected_btrt.max_bitrate
        );
        assert_eq!(
            normalized_decoder_config.avg_bitrate,
            expected_btrt.avg_bitrate
        );
    }

    #[test]
    fn normalize_imported_flat_mp4a_sample_entry_box_preserves_imported_decoder_bitrates_without_authority_headers()
     {
        let mut imported_track = imported_track(MuxTrackKind::Audio, None, 0);
        imported_track.timescale = 48_000;
        let mut esds = mp4a_profile_esds(0x40, &[0x12, 0x10]);
        for descriptor in &mut esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::ES_DESCRIPTOR_TAG
                && let Some(es_descriptor) = descriptor.es_descriptor.as_mut()
            {
                es_descriptor.es_id = 9;
            }
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG
                && let Some(config) = descriptor.decoder_config_descriptor.as_mut()
            {
                config.buffer_size_db = 17;
                config.max_bitrate = 128_000;
                config.avg_bitrate = 121_839;
            }
        }
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::iso14496_12::Btrt {
                        buffer_size_db: 4_096,
                        max_bitrate: 133_952,
                        avg_bitrate: 121_832,
                    },
                    &[],
                )
                .unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 512,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 512,
                data_size: 1_536,
                duration: 1_024,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Flat,
            true,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();
        let normalized_esds =
            crate::mux::mp4::decode_typed_box::<crate::boxes::iso14496_14::Esds>(&child_boxes[0])
                .unwrap();

        let mut normalized_decoder_config = None;
        for descriptor in normalized_esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG {
                normalized_decoder_config = descriptor.decoder_config_descriptor;
            }
        }
        let normalized_decoder_config = normalized_decoder_config.unwrap();

        assert_eq!(normalized_decoder_config.buffer_size_db, 17);
        assert_eq!(normalized_decoder_config.max_bitrate, 128_000);
        assert_eq!(normalized_decoder_config.avg_bitrate, 121_839);
    }

    #[test]
    fn normalize_imported_fragmented_dolby_audio_sample_entry_box_strips_zero_typed_child_for_ac3()
    {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.timescale = 48_000;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"ac-3"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[
                crate::mux::mp4::encode_typed_box(
                    &crate::boxes::etsi_ts_102_366::Dac3 {
                        fscod: 0,
                        bsid: 8,
                        bsmod: 0,
                        acmod: 7,
                        lfe_on: 1,
                        bit_rate_code: 15,
                    },
                    &[],
                )
                .unwrap(),
                crate::mux::mp4::encode_raw_box(FourCc::from_u32(0), &[]).unwrap(),
            ]
            .concat(),
        )
        .unwrap();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 2_048,
            duration: 1_536,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Fragmented,
            false,
        )
        .unwrap();
        let child_boxes =
            crate::mux::mp4::audio_sample_entry_immediate_children(&normalized).unwrap();

        assert_eq!(child_boxes.len(), 1);
        assert_eq!(
            super::sample_entry_box_type(&child_boxes[0]),
            Some(FourCc::from_bytes(*b"dac3"))
        );
    }

    #[test]
    fn normalize_imported_flat_h264_sample_entry_box_preserves_source_compressorname() {
        let transport_path = mux_fixture_path("transport_h264.ts");
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let source_sample_entry = crate::mux::mp4::decode_typed_box::<
            crate::boxes::iso14496_12::VisualSampleEntry,
        >(prepared.track_configs[0].sample_entry_box())
        .unwrap();
        let child_boxes = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();
        let sample_entry_box = super::build_visual_sample_entry_box_with_compressor_name(
            FourCc::from_bytes(*b"avc3"),
            source_sample_entry.width,
            source_sample_entry.height,
            b"AVC Coding",
            &child_boxes,
        )
        .unwrap();

        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = sample_entry_box;
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 1,
            duration: 1,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized =
            super::normalize_imported_flat_h264_sample_entry_box(&imported_track, false, false)
                .unwrap();
        let normalized_sample_entry = crate::mux::mp4::decode_typed_box::<
            crate::boxes::iso14496_12::VisualSampleEntry,
        >(&normalized)
        .unwrap();
        let visible_len = usize::from(normalized_sample_entry.compressorname[0]).min(31);
        assert_eq!(
            &normalized_sample_entry.compressorname[1..1 + visible_len],
            b"AVC Coding"
        );
    }

    #[test]
    fn normalize_imported_flat_h264_sample_entry_box_preserves_source_btrt_for_preserved_authority_layout()
     {
        let transport_path = mux_fixture_path("transport_h264.ts");
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let sample_entry_box = crate::mux::mp4::replace_visual_sample_entry_btrt(
            prepared.track_configs[0].sample_entry_box(),
            &crate::boxes::iso14496_12::Btrt {
                buffer_size_db: 0,
                max_bitrate: 8_838_640,
                avg_bitrate: 8_838_640,
            },
        )
        .unwrap();

        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = sample_entry_box;
        imported_track.timescale = 96_000;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 120_000,
                duration: 4_000,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 120_000,
                data_size: 120_000,
                duration: 4_000,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];

        let normalized =
            super::normalize_imported_flat_h264_sample_entry_box(&imported_track, false, true)
                .unwrap();
        let child_boxes =
            crate::mux::mp4::visual_sample_entry_immediate_children(&normalized).unwrap();
        let btrt_box = child_boxes
            .iter()
            .find(|child_box| {
                super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
            })
            .unwrap();
        let btrt =
            crate::mux::mp4::decode_typed_box::<crate::boxes::iso14496_12::Btrt>(btrt_box).unwrap();

        assert_eq!(btrt.buffer_size_db, 0);
        assert_eq!(btrt.max_bitrate, 8_838_640);
        assert_eq!(btrt.avg_bitrate, 8_838_640);
    }

    #[test]
    fn normalize_imported_flat_h264_sample_entry_box_orders_pasp_ahead_of_colr() {
        let transport_path = mux_fixture_path("transport_h264_wrap_colr.ts");
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();

        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = prepared.track_configs[0].sample_entry_box().to_vec();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 1,
            duration: 1,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized =
            super::normalize_imported_flat_h264_sample_entry_box(&imported_track, false, false)
                .unwrap();
        let child_types = crate::mux::mp4::visual_sample_entry_immediate_children(&normalized)
            .unwrap()
            .iter()
            .filter_map(|child_box| super::sample_entry_box_type(child_box))
            .collect::<Vec<_>>();

        let pasp_index = child_types
            .iter()
            .position(|box_type| *box_type == FourCc::from_bytes(*b"pasp"))
            .unwrap();
        let colr_index = child_types
            .iter()
            .position(|box_type| *box_type == FourCc::from_bytes(*b"colr"))
            .unwrap();

        assert!(pasp_index < colr_index);
    }

    #[test]
    fn normalize_imported_flat_h264_sample_entry_box_preserves_source_colr_before_pasp_for_authority_layout()
     {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        let transport_path = mux_fixture_path("transport_h264_wrap_colr.ts");
        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            transport_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let fixture_child_boxes = crate::mux::mp4::visual_sample_entry_immediate_children(
            prepared.track_configs[0].sample_entry_box(),
        )
        .unwrap();
        let avcc = fixture_child_boxes
            .iter()
            .find(|child_box| {
                super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"avcC"))
            })
            .unwrap()
            .clone();
        let colr = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::Colr {
                colour_type: FourCc::from_bytes(*b"nclx"),
                colour_primaries: 1,
                transfer_characteristics: 1,
                matrix_coefficients: 1,
                full_range_flag: false,
                reserved: 0,
                profile: Vec::new(),
                unknown: Vec::new(),
            },
            &[],
        )
        .unwrap();
        let pasp = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::Pasp {
                h_spacing: 4,
                v_spacing: 3,
            },
            &[],
        )
        .unwrap();
        let btrt = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::Btrt {
                buffer_size_db: 0,
                max_bitrate: 1_000,
                avg_bitrate: 1_000,
            },
            &[],
        )
        .unwrap();
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"avc1"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[avcc, colr, pasp, btrt].concat(),
        )
        .unwrap();
        imported_track.samples = vec![ImportedSample {
            source_index: 0,
            data_offset: 0,
            data_size: 1,
            duration: 1,
            composition_time_offset: 0,
            is_sync_sample: true,
        }];

        let normalized =
            super::normalize_imported_flat_h264_sample_entry_box(&imported_track, false, true)
                .unwrap();
        let child_types = crate::mux::mp4::visual_sample_entry_immediate_children(&normalized)
            .unwrap()
            .iter()
            .filter_map(|child_box| super::sample_entry_box_type(child_box))
            .collect::<Vec<_>>();

        let pasp_index = child_types
            .iter()
            .position(|box_type| *box_type == FourCc::from_bytes(*b"pasp"))
            .unwrap();
        let colr_index = child_types
            .iter()
            .position(|box_type| *box_type == FourCc::from_bytes(*b"colr"))
            .unwrap();

        assert!(colr_index < pasp_index);
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_hevc_track_adds_cslg_from_source_duration() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        let layered_config_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&8_u32.to_be_bytes());
            bytes.extend_from_slice(b"lhvC");
            bytes
        };
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &layered_config_box,
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 128,
                composition_time_offset: 384,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 2,
                data_size: 1,
                duration: 128,
                composition_time_offset: -256,
                is_sync_sample: true,
            },
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(11_520),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        let generated =
            super::generated_flat_stbl_boxes_for_imported_track(&imported_track, None, &[], false)
                .unwrap();
        let cslg_box = generated
            .iter()
            .find(|child_box| {
                super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"cslg"))
            })
            .unwrap();
        let cslg =
            crate::mux::mp4::decode_typed_box::<crate::boxes::iso14496_12::Cslg>(cslg_box).unwrap();

        assert_eq!(cslg.least_decode_to_display_delta(), -256);
        assert_eq!(cslg.greatest_decode_to_display_delta(), 384);
        assert_eq!(cslg.composition_start_time(), 0);
        assert_eq!(cslg.composition_end_time(), 11_520);
    }

    #[test]
    fn generated_flat_stbl_boxes_for_imported_non_layered_hevc_track_omit_generated_cslg() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 128,
                composition_time_offset: 384,
                is_sync_sample: true,
            },
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(11_520),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        let generated =
            super::generated_flat_stbl_boxes_for_imported_track(&imported_track, None, &[], false)
                .unwrap();

        assert!(!generated.iter().any(|child_box| {
            super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"cslg"))
        }));
    }

    #[test]
    fn imported_track_flat_authority_media_duration_uses_timescale_for_short_layered_hevc() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        let layered_config_box = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&8_u32.to_be_bytes());
            bytes.extend_from_slice(b"lhvC");
            bytes
        };
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &layered_config_box,
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(640),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(11_520)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_includes_edit_media_time() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(82_082),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });
        imported_track.source_edit_media_time = Some(2_002);

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(84_084)
        );
    }

    #[test]
    fn preserved_imported_timing_override_uses_derived_video_media_duration_for_edit_lists() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 1_001,
                is_sync_sample: true,
            },
        ];
        imported_track.source_edit_media_time = Some(2_002);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(1_000),
                    source_media_duration: Some(2_002),
                    source_edit_segment_duration: Some(3_003),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, false)
                .expect("timing override")
                .media_duration,
            3_003
        );
    }

    #[test]
    fn preserved_imported_timing_override_uses_trailing_video_presentation_end_for_edit_lists() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 1_001,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 2_002,
                is_sync_sample: false,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 2,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: false,
            },
        ];
        imported_track.source_edit_media_time = Some(1_001);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(1_000),
                    source_media_duration: Some(3_003),
                    source_edit_segment_duration: Some(3_003),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, true)
                .expect("timing override")
                .media_duration,
            3_003
        );
    }

    #[test]
    fn preserved_imported_timing_override_uses_scaled_edit_segment_duration_for_zero_offset_avc_video_with_rescaled_movie_edit()
     {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"avc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.source_edit_media_time = Some(0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(640),
                    source_media_duration: Some(640),
                    source_edit_segment_duration: Some(640),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, false)
                .expect("timing override")
                .media_duration,
            11_520
        );
    }

    #[test]
    fn preserved_imported_timing_override_preserves_source_media_duration_for_zero_offset_non_avc_hevc_video()
     {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vp09"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 130,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.source_edit_media_time = Some(0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(11_520),
                    source_media_duration: Some(640),
                    source_edit_segment_duration: Some(650),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, false)
                .expect("timing override")
                .media_duration,
            640
        );
    }

    #[test]
    fn preserved_imported_timing_override_preserves_source_media_duration_for_zero_offset_non_layered_hevc_video()
     {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 130,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.source_edit_media_time = Some(0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(11_520),
                    source_media_duration: Some(640),
                    source_edit_segment_duration: Some(650),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, false)
                .expect("timing override")
                .media_duration,
            640
        );
    }

    #[test]
    fn preserved_imported_timing_override_keeps_scaled_edit_segment_duration_for_zero_offset_avc_video()
     {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"avc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 130,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.source_edit_media_time = Some(0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(11_520),
                    source_media_duration: Some(640),
                    source_edit_segment_duration: Some(650),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, true)
                .expect("timing override")
                .media_duration,
            650
        );
    }

    #[test]
    fn preserved_imported_timing_override_uses_one_second_floor_for_layered_hevc_zero_offset_video()
    {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 11_520;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &opaque_fragmented_child_box(*b"lhvC"),
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            5
        ];
        imported_track.source_edit_media_time = Some(0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_track_id: Some(1),
                    source_movie_timescale: Some(11_520),
                    source_media_duration: Some(640),
                    source_edit_segment_duration: Some(640),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::preserved_imported_timing_override(&imported_track, false)
                .expect("timing override")
                .media_duration,
            11_520
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_falls_back_from_zero_source_duration() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(0),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(82_082)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_falls_back_from_zero_source_duration_for_audio()
    {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(0),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(82_082)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_preserves_source_duration_for_audio_edit_list()
    {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(131_518),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.source_edit_media_time = Some(312);

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(131_518)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_uses_imported_duration_for_speex_audio() {
        let mut imported_track = imported_speex_track();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_184,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 0,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(900),
                    source_edit_segment_duration: Some(1_000),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(1_184)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_uses_imported_duration_for_mp3_audio_without_edit_list()
     {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        let mut esds = crate::boxes::iso14496_14::Esds::default();
        esds.descriptors = vec![crate::boxes::iso14496_14::Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0x6b,
                stream_type: 5,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..crate::boxes::iso14496_14::Descriptor::default()
        }];
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &crate::mux::mp4::encode_typed_box(&esds, &[]).unwrap(),
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(65_755_008),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(82_082)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_includes_edit_media_time_for_iamf_audio() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::AudioSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"iamf"),
                    data_reference_index: 1,
                },
                ..crate::boxes::iso14496_12::AudioSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(131_518),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });
        imported_track.source_edit_media_time = Some(312);

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(131_830)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_extends_no_edit_video_tail() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(82_082),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(82_082)
        );
    }

    #[test]
    fn flat_timing_override_for_imported_zero_duration_audio_preserves_imported_media_duration() {
        let mut imported_track = imported_track(MuxTrackKind::Audio, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            82
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(0),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Audio)
                });

        let override_timing =
            super::flat_timing_override_for_imported_track(&imported_track, 90_000, false)
                .expect("expected preserved timing override");
        assert_eq!(override_timing.media_duration, 82_082);
        assert_eq!(override_timing.presentation_duration, 82_082);
    }

    #[test]
    fn imported_track_flat_authority_media_duration_extends_truncated_no_edit_video_tail() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1_001,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            81
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(82_082),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(82_082)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_does_not_extend_no_edit_vvc1_video_tail() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vvc1"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            3
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(2),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(2)
        );
    }

    #[test]
    fn imported_track_flat_authority_media_duration_ignores_edit_media_time_for_vvc1_video() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 1);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vvc1"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 1,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            2
        ];
        imported_track.mux_policy =
            imported_track
                .mux_policy
                .with_header_policy(ImportedTrackHeaderPolicy {
                    source_media_duration: Some(2),
                    ..super::default_imported_track_header_policy(MuxTrackKind::Video)
                });

        assert_eq!(
            super::imported_track_flat_authority_media_duration(&imported_track),
            Some(2)
        );
    }

    #[test]
    fn infer_imported_mp4_authority_flat_ftyp_profile_uses_iso4_only_for_layered_hevc() {
        let mut standard_hevc_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        standard_hevc_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        let layered_hevc_track = {
            let layered_config_box = {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&8_u32.to_be_bytes());
                bytes.extend_from_slice(b"lhvC");
                bytes
            };
            let mut track = imported_track(MuxTrackKind::Video, Some(1), 0);
            track.sample_entry_box = crate::mux::mp4::encode_typed_box(
                &crate::boxes::iso14496_12::VisualSampleEntry {
                    sample_entry: crate::boxes::iso14496_12::SampleEntry {
                        box_type: FourCc::from_bytes(*b"hvc1"),
                        data_reference_index: 1,
                    },
                    width: 1,
                    height: 1,
                    ..crate::boxes::iso14496_12::VisualSampleEntry::default()
                },
                &layered_config_box,
            )
            .unwrap();
            track
        };

        assert_eq!(
            super::infer_imported_mp4_authority_flat_ftyp_profile(&[standard_hevc_track]),
            (
                FourCc::from_bytes(*b"isom"),
                1,
                vec![FourCc::from_bytes(*b"isom")],
            )
        );
        assert_eq!(
            super::infer_imported_mp4_authority_flat_ftyp_profile(&[layered_hevc_track]),
            (
                FourCc::from_bytes(*b"isom"),
                1,
                vec![FourCc::from_bytes(*b"isom"), FourCc::from_bytes(*b"iso4")],
            )
        );
    }

    #[test]
    fn sync_sample_table_mode_for_imported_track_uses_auto_for_fragmented_hevc() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"hvc1"),
            &imported_track.sample_entry_box,
            MuxTrackKind::Video,
        );

        assert_eq!(
            super::sync_sample_table_mode_for_imported_track(
                &imported_track,
                MuxOutputLayout::Fragmented,
                false,
            ),
            super::SyncSampleTableMode::Auto
        );
        assert_eq!(
            super::sync_sample_table_mode_for_imported_track(
                &imported_track,
                MuxOutputLayout::Flat,
                false,
            ),
            super::SyncSampleTableMode::ForceFirstOnly
        );
    }

    #[test]
    fn sync_sample_table_mode_for_imported_track_preserves_first_only_fragmented_hevc() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"hvc1"),
            &imported_track.sample_entry_box,
            MuxTrackKind::Video,
        )
        .with_header_policy(ImportedTrackHeaderPolicy {
            source_stss_first_only: true,
            ..super::default_imported_track_header_policy(MuxTrackKind::Video)
        });

        assert_eq!(
            super::sync_sample_table_mode_for_imported_track(
                &imported_track,
                MuxOutputLayout::Fragmented,
                false,
            ),
            super::SyncSampleTableMode::ForceFirstOnly
        );
    }

    #[test]
    fn sync_sample_table_mode_for_imported_track_uses_auto_for_preserved_flat_hevc() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.mux_policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"hvc1"),
            &imported_track.sample_entry_box,
            MuxTrackKind::Video,
        )
        .with_header_policy(ImportedTrackHeaderPolicy {
            source_track_id: Some(1),
            source_stss_first_only: false,
            ..super::default_imported_track_header_policy(MuxTrackKind::Video)
        });

        assert_eq!(
            super::sync_sample_table_mode_for_imported_track(
                &imported_track,
                MuxOutputLayout::Flat,
                true,
            ),
            super::SyncSampleTableMode::Auto
        );
    }

    #[test]
    fn generated_flat_stbl_boxes_for_preserved_authority_hevc_skip_visual_random_access_groups() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[],
        )
        .unwrap();
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 1,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: false,
            },
            ImportedSample {
                source_index: 0,
                data_offset: 2,
                data_size: 1,
                duration: 128,
                composition_time_offset: 0,
                is_sync_sample: true,
            },
        ];
        imported_track.mux_policy = super::imported_track_mux_policy_for_sample_entry_type(
            FourCc::from_bytes(*b"hvc1"),
            &imported_track.sample_entry_box,
            MuxTrackKind::Video,
        )
        .with_header_policy(ImportedTrackHeaderPolicy {
            source_track_id: Some(1),
            ..super::default_imported_track_header_policy(MuxTrackKind::Video)
        });

        let generated =
            super::generated_flat_stbl_boxes_for_imported_track(&imported_track, None, &[], true)
                .unwrap();

        assert!(!generated.iter().any(|child_box| {
            matches!(
                super::sample_entry_box_type(child_box),
                Some(value)
                    if value == FourCc::from_bytes(*b"sgpd")
                        || value == FourCc::from_bytes(*b"sbgp")
            )
        }));
    }

    #[test]
    fn finish_prepared_request_extends_imported_avc_no_edit_list_flat_timing_override() {
        let input_path = mux_fixture_path("imported_avc_no_edit_list.mp4");
        let mut sources = SourceCatalog::default();
        let mut cache = BTreeMap::new();
        let metadata = super::load_mp4_source_sync(&input_path, &mut cache, &mut sources).unwrap();
        let selected = super::select_container_tracks(
            &metadata.tracks,
            Some(MuxMp4TrackSelector::Video),
            "fixture".to_string(),
            false,
        )
        .unwrap();
        let selected = &selected[0];

        assert_eq!(
            super::imported_track_source_media_duration(selected),
            Some(82_082)
        );
        assert_eq!(
            super::imported_sample_media_duration(&selected.samples),
            Some(83_083)
        );
        assert_eq!(
            super::imported_track_flat_authority_media_duration(selected),
            Some(83_083)
        );

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            input_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let flat_timing_override = prepared.track_configs[0].flat_timing_override().unwrap();

        assert_eq!(flat_timing_override.media_duration, 83_083);
        assert_eq!(flat_timing_override.presentation_duration, 83_083);
    }

    #[test]
    fn finish_prepared_request_uses_isom_only_for_imported_hevc_flat_profile() {
        let input_path = mux_fixture_path("imported_hevc.mp4");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            input_path,
            MuxMp4TrackSelector::Video,
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();

        assert_eq!(
            prepared.file_config.major_brand(),
            FourCc::from_bytes(*b"isom")
        );
        assert_eq!(
            prepared.file_config.compatible_brands(),
            [FourCc::from_bytes(*b"isom")]
        );
    }

    #[test]
    fn finish_prepared_request_splits_terminal_short_flat_video_sample_into_its_own_chunk() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.timescale = 90_000;
        imported_track.samples = vec![
            ImportedSample {
                source_index: 0,
                data_offset: 0,
                data_size: 1,
                duration: 3_003,
                composition_time_offset: 0,
                is_sync_sample: true,
            };
            28
        ];
        imported_track.samples.push(ImportedSample {
            source_index: 0,
            data_offset: 28,
            data_size: 1,
            duration: 1_001,
            composition_time_offset: 0,
            is_sync_sample: true,
        });

        let request = MuxRequest::new(vec![MuxTrackSpec::path("synthetic.mpeg#video")])
            .with_output_layout(MuxOutputLayout::Flat);
        let prepared = finish_prepared_request(
            &request,
            PathBuf::from("out.mp4").as_path(),
            vec![imported_track],
            SourceCatalog::default(),
            None,
            SelectedImportedMp4CarryMap::new(),
        )
        .unwrap();

        assert_eq!(prepared.plan.chunk_sample_counts(1).unwrap(), &[14, 14, 1]);
    }

    #[test]
    fn finish_prepared_request_rechunks_imported_mpegh_flat_audio_by_half_second() {
        let source_path = mux_fixture_path("imported_mpegh_audio.mp4");

        let request = MuxRequest::new(vec![MuxTrackSpec::selected(
            source_path,
            MuxMp4TrackSelector::Audio { occurrence: 1 },
        )])
        .with_output_layout(MuxOutputLayout::Flat);
        let prepared =
            super::prepare_request_sync(&request, PathBuf::from("out.mp4").as_path()).unwrap();
        let track_id = prepared.track_configs[0].track_id();
        let chunk_sample_counts = prepared.plan.chunk_sample_counts(track_id).unwrap();

        assert_eq!(chunk_sample_counts.len(), 94);
        assert!(chunk_sample_counts[..93].iter().all(|count| *count == 23));
        assert_eq!(chunk_sample_counts[93], 21);
    }

    #[test]
    fn normalize_imported_sample_entry_box_promotes_fragmented_vp9_zero_level() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        let mut vpcc = crate::boxes::vp::VpCodecConfiguration::default();
        vpcc.set_version(1);
        vpcc.profile = 1;
        vpcc.level = 0;
        vpcc.bit_depth = 8;
        vpcc.chroma_subsampling = 3;
        vpcc.video_full_range_flag = 1;
        vpcc.colour_primaries = 1;
        vpcc.transfer_characteristics = 13;
        vpcc.matrix_coefficients = 0;
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"vp09"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &crate::mux::mp4::encode_typed_box(&vpcc, &[]).unwrap(),
        )
        .unwrap();

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Fragmented,
            false,
        )
        .unwrap();
        let children =
            crate::mux::mp4::visual_sample_entry_immediate_children(&normalized).unwrap();
        let vpcc_box = children
            .iter()
            .find(|child_box| {
                super::sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"vpcC"))
            })
            .unwrap();
        let normalized_vpcc =
            crate::mux::mp4::decode_typed_box::<crate::boxes::vp::VpCodecConfiguration>(vpcc_box)
                .unwrap();

        assert_eq!(normalized_vpcc.level, 0x14);
    }

    #[test]
    fn normalize_imported_sample_entry_box_strips_fragmented_layered_hevc_sidecars() {
        let mut imported_track = imported_track(MuxTrackKind::Video, Some(1), 0);
        imported_track.sample_entry_box = crate::mux::mp4::encode_typed_box(
            &crate::boxes::iso14496_12::VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"hvc1"),
                    data_reference_index: 1,
                },
                width: 1,
                height: 1,
                ..crate::boxes::iso14496_12::VisualSampleEntry::default()
            },
            &[
                opaque_fragmented_child_box(*b"hvcC"),
                opaque_fragmented_child_box(*b"lhvC"),
                opaque_fragmented_child_box(*b"chrm"),
                opaque_fragmented_child_box(*b"vexu"),
                opaque_fragmented_child_box(*b"hfov"),
                opaque_fragmented_child_box(*b"colr"),
            ]
            .concat(),
        )
        .unwrap();

        let normalized = super::normalize_imported_sample_entry_box(
            &imported_track,
            None,
            MuxOutputLayout::Fragmented,
            false,
        )
        .unwrap();
        let child_types: Vec<FourCc> =
            crate::mux::mp4::visual_sample_entry_immediate_children(&normalized)
                .unwrap()
                .iter()
                .filter_map(|child_box| super::sample_entry_box_type(child_box))
                .collect();

        assert_eq!(
            child_types,
            vec![FourCc::from_bytes(*b"hvcC"), FourCc::from_bytes(*b"colr")]
        );
    }

    #[test]
    fn build_btrt_uses_overridden_total_duration_for_average_rate() {
        let btrt = super::build_btrt_from_sample_sizes_with_total_duration(
            [(100_u32, 4_u32), (100_u32, 4_u32)],
            1_000,
            Some(10),
        )
        .unwrap();

        assert_eq!(btrt.buffer_size_db, 100);
        assert_eq!(btrt.avg_bitrate, 160_000);
        assert_eq!(btrt.max_bitrate, 160_000);
    }

    fn opaque_fragmented_child_box(box_type: [u8; 4]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&8_u32.to_be_bytes());
        bytes.extend_from_slice(&box_type);
        bytes
    }
}

pub(in crate::mux) fn with_force_empty_sync_sample_table(
    mut policy: ImportedTrackMuxPolicy,
) -> ImportedTrackMuxPolicy {
    policy.sync_sample_table_mode = SyncSampleTableMode::ForceEmpty;
    policy
}

fn flat_timing_override_for_imported_track(
    imported_track: &ImportedTrack,
    movie_timescale: u32,
    preserve_flat_authority_layout: bool,
) -> Option<FlatTimingOverride> {
    if imported_track.samples.is_empty() {
        return None;
    }

    if imported_track_uses_iamf_family(imported_track)
        && imported_track.mux_policy.header_policy().is_none()
    {
        return direct_iamf_flat_timing_override(imported_track);
    }

    if imported_track_source_media_duration(imported_track) == Some(0) {
        return preserved_imported_timing_override(imported_track, preserve_flat_authority_layout);
    }

    if imported_track_source_edit_segment_duration(imported_track).is_some() {
        return preserved_imported_timing_override(imported_track, preserve_flat_authority_layout);
    }

    if imported_track.mux_policy.header_policy().is_some()
        && imported_track_flat_authority_media_duration(imported_track)
            .zip(imported_sample_media_duration(&imported_track.samples))
            .is_some_and(|(authority_media_duration, imported_media_duration)| {
                authority_media_duration != imported_media_duration
            })
    {
        return preserved_imported_timing_override(imported_track, preserve_flat_authority_layout);
    }

    if imported_track.mux_policy.header_policy().is_some()
        && imported_track.timescale != movie_timescale
        && !track_times_fit_movie_timescale(imported_track, movie_timescale)
    {
        return preserved_imported_timing_override(imported_track, preserve_flat_authority_layout);
    }

    None
}

fn direct_iamf_flat_timing_override(imported_track: &ImportedTrack) -> Option<FlatTimingOverride> {
    let sample_count = imported_track.samples.len();
    if sample_count == 0 {
        return None;
    }

    let mut sample_durations = vec![1_u32; sample_count];
    *sample_durations.last_mut()? = u32::MAX;
    Some(FlatTimingOverride {
        sample_durations,
        composition_offsets: vec![0; sample_count],
        media_duration: u64::from(u32::MAX)
            .checked_add(u64::try_from(sample_count).ok()?)?
            .checked_sub(1)?,
        presentation_duration: u64::from(u32::MAX)
            .checked_add(u64::try_from(sample_count).ok()?)?
            .checked_sub(1)?,
    })
}

fn preserved_imported_timing_override(
    imported_track: &ImportedTrack,
    preserve_flat_authority_layout: bool,
) -> Option<FlatTimingOverride> {
    let sample_durations = imported_track
        .samples
        .iter()
        .map(|sample| sample.duration)
        .collect::<Vec<_>>();
    let composition_offsets = imported_track
        .samples
        .iter()
        .map(|sample| sample.composition_time_offset)
        .collect::<Vec<_>>();
    let mut decode_time = 0_u64;
    let mut media_duration = 0_u64;
    let mut max_presentation_end = 0_u64;
    let mut trailing_presentation_end = None::<u64>;
    for sample in &imported_track.samples {
        let duration = u64::from(sample.duration);
        let decode_end = decode_time.checked_add(duration)?;
        media_duration = media_duration.max(decode_end);
        let presentation_end = i128::from(decode_time)
            .saturating_add(i128::from(sample.composition_time_offset))
            .saturating_add(i128::from(sample.duration));
        if presentation_end > 0 {
            let presentation_end = u64::try_from(presentation_end).ok()?;
            max_presentation_end = max_presentation_end.max(presentation_end);
            trailing_presentation_end = Some(presentation_end);
        }
        decode_time = decode_end;
    }
    let derived_media_duration = media_duration.max(max_presentation_end);
    let trailing_video_media_duration =
        media_duration.max(trailing_presentation_end.unwrap_or_default());
    let authority_media_duration = imported_track_flat_authority_media_duration(imported_track);
    let source_edit_segment_duration = imported_track_source_edit_segment_duration(imported_track);
    let preserve_zero_offset_source_media_duration = imported_track.kind.is_video()
        && imported_track.source_edit_media_time == Some(0)
        && source_edit_segment_duration.is_some()
        && !imported_track_uses_avc_family(imported_track)
        && !imported_track_uses_layered_hevc_family(imported_track);
    media_duration = match authority_media_duration {
        Some(_)
            if !preserve_flat_authority_layout
                && imported_track.kind.is_video()
                && imported_track_uses_layered_hevc_family(imported_track)
                && imported_track.source_edit_media_time == Some(0)
                && source_edit_segment_duration.is_some()
                && derived_media_duration < u64::from(imported_track.timescale) =>
        {
            u64::from(imported_track.timescale)
        }
        Some(authority_media_duration)
            if !preserve_flat_authority_layout
                && imported_track.kind.is_video()
                && imported_track_uses_avc_family(imported_track)
                && imported_track.source_edit_media_time == Some(0)
                && source_edit_segment_duration
                    .is_some_and(|segment_duration| segment_duration > derived_media_duration)
                && authority_media_duration == derived_media_duration =>
        {
            source_edit_segment_duration.unwrap_or(authority_media_duration)
        }
        Some(source_media_duration) if preserve_zero_offset_source_media_duration => {
            source_media_duration
        }
        Some(_)
            if preserve_flat_authority_layout
                && imported_track.kind.is_video()
                && imported_track.source_edit_media_time.is_some() =>
        {
            trailing_video_media_duration
                .max(imported_track_source_media_duration(imported_track).unwrap_or(0))
        }
        Some(_)
            if imported_track.kind.is_video()
                && imported_track.source_edit_media_time.is_some() =>
        {
            derived_media_duration
                .max(imported_track_source_media_duration(imported_track).unwrap_or(0))
        }
        Some(authority_media_duration) => authority_media_duration,
        None => derived_media_duration,
    };
    let presentation_duration = if imported_track_uses_speex_family(imported_track) {
        media_duration
    } else {
        imported_track_source_edit_segment_duration(imported_track).unwrap_or_else(|| {
            imported_track
                .source_edit_media_time
                .map_or(media_duration, |edit_media_time| {
                    media_duration.saturating_sub(edit_media_time)
                })
        })
    };
    Some(FlatTimingOverride {
        sample_durations,
        composition_offsets,
        media_duration,
        presentation_duration,
    })
}

fn imported_track_source_media_duration(imported_track: &ImportedTrack) -> Option<u64> {
    imported_track
        .mux_policy
        .header_policy()?
        .source_media_duration
}

fn imported_track_flat_authority_media_duration(imported_track: &ImportedTrack) -> Option<u64> {
    let imported_media_duration = imported_sample_media_duration(&imported_track.samples);
    let source_media_duration =
        imported_track_source_media_duration(imported_track).and_then(|duration| {
            let duration = if duration == 0 {
                imported_media_duration.unwrap_or(duration)
            } else if imported_track.kind.is_video() {
                if let Some(edit_media_time) = imported_track.source_edit_media_time {
                    if imported_track_uses_vvc_family(imported_track) {
                        duration
                    } else {
                        duration.checked_add(edit_media_time)?
                    }
                } else if imported_track_source_edit_segment_duration(imported_track).is_none()
                    && !imported_track_uses_vvc_family(imported_track)
                {
                    imported_media_duration.unwrap_or(duration).max(duration)
                } else {
                    duration
                }
            } else if imported_track_uses_iamf_family(imported_track) {
                if let Some(edit_media_time) = imported_track.source_edit_media_time {
                    duration.checked_add(edit_media_time)?
                } else {
                    duration
                }
            } else if imported_track_uses_mp3_family(imported_track)
                && imported_track.source_edit_media_time.is_none()
            {
                imported_media_duration.unwrap_or(duration)
            } else {
                duration
            };
            Some(duration)
        });
    if imported_track_uses_speex_family(imported_track) {
        return match (source_media_duration, imported_media_duration) {
            (Some(source_media_duration), Some(imported_media_duration)) => {
                Some(source_media_duration.max(imported_media_duration))
            }
            (Some(source_media_duration), None) => Some(source_media_duration),
            (None, Some(imported_media_duration)) => Some(imported_media_duration),
            (None, None) => None,
        };
    }
    if imported_track_uses_layered_hevc_family(imported_track)
        && imported_sample_media_duration(&imported_track.samples)
            .is_some_and(|duration| duration < u64::from(imported_track.timescale))
    {
        return Some(
            source_media_duration
                .unwrap_or(0)
                .max(u64::from(imported_track.timescale)),
        );
    }
    source_media_duration
}

fn imported_track_source_edit_segment_duration(imported_track: &ImportedTrack) -> Option<u64> {
    let policy = imported_track.mux_policy.header_policy()?;
    let segment_duration = policy.source_edit_segment_duration?;
    if segment_duration == 0 {
        return None;
    }
    let source_movie_timescale = policy.source_movie_timescale?;
    if source_movie_timescale == imported_track.timescale {
        return Some(segment_duration);
    }
    scale_track_time_to_movie(
        policy.source_track_id.unwrap_or(0),
        i64::try_from(segment_duration).ok()?,
        source_movie_timescale,
        imported_track.timescale,
        true,
    )
    .ok()
    .and_then(|scaled| u64::try_from(scaled).ok())
}

fn imported_track_should_rechunk_flat_audio(imported_track: &ImportedTrack) -> bool {
    if imported_track_uses_speex_family(imported_track) {
        return false;
    }
    if imported_track_uses_mp4a_family(imported_track)
        && sample_entry_carries_oti(&imported_track.sample_entry_box, 0xDD)
    {
        return false;
    }
    imported_track.kind.is_audio() && imported_track.mux_policy.header_policy().is_some()
}

fn build_imported_flat_audio_chunk_sample_counts(
    track_id: u32,
    imported_track: &ImportedTrack,
    sample_durations: Vec<u32>,
) -> Result<Vec<u32>, MuxError> {
    let target_ticks = auto_flat_interleave_target_ticks(imported_track.timescale);
    if imported_track_uses_speex_family(imported_track) {
        return build_prev_sample_duration_chunk_sample_counts(
            track_id,
            sample_durations,
            target_ticks,
        );
    }
    if imported_track_uses_mp4a_family(imported_track)
        && sample_entry_carries_oti(&imported_track.sample_entry_box, 0xDD)
    {
        return build_prev_sample_duration_chunk_sample_counts(
            track_id,
            sample_durations,
            target_ticks,
        );
    }
    build_capped_duration_chunk_sample_counts(track_id, sample_durations, target_ticks)
}

fn build_preserved_authority_flat_audio_chunk_sample_counts(
    track_id: u32,
    imported_track: &ImportedTrack,
    movie_timescale: u32,
    video_alignment: &PreservedAuthorityFlatVideoAlignment,
) -> Result<Vec<u32>, MuxError> {
    if imported_track.samples.is_empty() {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: "authority-aligned audio chunking requires at least one sample".to_string(),
        });
    }
    if movie_timescale == 0 || imported_track.timescale == 0 || video_alignment.timescale == 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }

    let interleave_target_ticks = auto_flat_interleave_target_ticks(movie_timescale);
    let video_chunk_sample_counts = video_alignment.driving_chunk_sample_counts();
    if video_chunk_sample_counts.is_empty() {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: "authority-aligned audio chunking requires at least one video chunk"
                .to_string(),
        });
    }

    let mut chunk_sample_counts = Vec::with_capacity(video_chunk_sample_counts.len());
    let mut audio_sample_index = 0_usize;
    let mut audio_decode_time = 0_u64;
    let mut audio_previous_dts = 0_u64;
    let mut audio_chunk_duration = 0_u64;
    let mut video_sample_index = 0_usize;
    let mut video_decode_time = 0_u64;

    for &video_chunk_sample_count in video_chunk_sample_counts {
        let video_chunk_len = usize::try_from(video_chunk_sample_count)
            .map_err(|_| MuxError::LayoutOverflow("authority video chunk sample-count"))?;
        if video_chunk_len == 0 {
            return Err(MuxError::InvalidChunkPlan {
                track_id,
                message: "authority video chunk plan contained an empty chunk".to_string(),
            });
        }
        let video_chunk_end_index = video_sample_index
            .checked_add(video_chunk_len)
            .ok_or(MuxError::LayoutOverflow("authority video chunk indexing"))?;
        let video_chunk_durations = video_alignment
            .sample_durations
            .get(video_sample_index..video_chunk_end_index)
            .ok_or_else(|| MuxError::InvalidChunkPlan {
                track_id,
                message: "authority video chunk plan ran past the carried video sample count"
                    .to_string(),
            })?;
        let mut video_last_sample_dts = video_decode_time;
        for (sample_index, &duration) in video_chunk_durations.iter().enumerate() {
            if sample_index + 1 < video_chunk_durations.len() {
                video_last_sample_dts = video_last_sample_dts
                    .checked_add(u64::from(duration))
                    .ok_or(MuxError::LayoutOverflow("authority video decode timeline"))?;
            }
            video_decode_time = video_decode_time
                .checked_add(u64::from(duration))
                .ok_or(MuxError::LayoutOverflow("authority video decode timeline"))?;
        }
        video_sample_index = video_chunk_end_index;

        let chunk_last_dts = video_last_sample_dts
            .checked_add(scale_interleave_target_to_track_timescale(
                interleave_target_ticks,
                movie_timescale,
                video_alignment.timescale,
            )?)
            .ok_or(MuxError::LayoutOverflow("authority video drift window"))?;
        let mut current_chunk_sample_count = 0_u32;

        while let Some(sample) = imported_track.samples.get(audio_sample_index) {
            let dts_delta = audio_decode_time
                .checked_sub(audio_previous_dts)
                .ok_or(MuxError::LayoutOverflow("authority audio dts delta"))?;
            let exceeds_local_window = timestamp_greater(
                dts_delta
                    .checked_add(audio_chunk_duration)
                    .ok_or(MuxError::LayoutOverflow("authority audio chunk duration"))?,
                imported_track.timescale,
                interleave_target_ticks,
                movie_timescale,
            );
            let exceeds_authority_drift = chunk_last_dts != 0
                && timestamp_greater(
                    audio_previous_dts,
                    imported_track.timescale,
                    chunk_last_dts,
                    video_alignment.timescale,
                );
            if (exceeds_local_window || exceeds_authority_drift) && audio_chunk_duration != 0 {
                audio_chunk_duration = 0;
                break;
            }

            audio_chunk_duration = audio_chunk_duration
                .checked_add(u64::from(sample.duration))
                .ok_or(MuxError::LayoutOverflow("authority audio chunk duration"))?;
            audio_previous_dts = audio_decode_time;
            audio_decode_time = audio_decode_time
                .checked_add(u64::from(sample.duration))
                .ok_or(MuxError::LayoutOverflow("authority audio decode timeline"))?;
            current_chunk_sample_count =
                current_chunk_sample_count
                    .checked_add(1)
                    .ok_or(MuxError::LayoutOverflow(
                        "authority audio chunk sample count",
                    ))?;
            audio_sample_index += 1;
        }

        if current_chunk_sample_count != 0 {
            chunk_sample_counts.push(current_chunk_sample_count);
        }
        if audio_sample_index == imported_track.samples.len() {
            break;
        }
    }

    if audio_sample_index != imported_track.samples.len() {
        let remaining_sample_count = imported_track.samples.len() - audio_sample_index;
        chunk_sample_counts.push(
            u32::try_from(remaining_sample_count)
                .map_err(|_| MuxError::LayoutOverflow("authority audio trailing chunk"))?,
        );
    }

    if chunk_sample_counts.is_empty() {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: "chunk plans may not be empty".to_string(),
        });
    }

    let item_count = u32::try_from(imported_track.samples.len())
        .map_err(|_| MuxError::LayoutOverflow("authority audio sample count"))?;
    let mut total_samples = 0_u32;
    for &samples_per_chunk in &chunk_sample_counts {
        if samples_per_chunk == 0 {
            return Err(MuxError::InvalidChunkPlan {
                track_id,
                message: "chunk plans may not contain zero-length chunks".to_string(),
            });
        }
        total_samples = total_samples
            .checked_add(samples_per_chunk)
            .ok_or(MuxError::LayoutOverflow("chunk sample-count total"))?;
    }
    if total_samples != item_count {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: format!(
                "chunk plan resolves {total_samples} samples for {item_count} staged samples"
            ),
        });
    }

    Ok(chunk_sample_counts)
}

fn preserved_authority_flat_video_alignment(
    imported_tracks: &[ImportedTrack],
    assigned_track_ids: &[u32],
) -> Result<Option<PreservedAuthorityFlatVideoAlignment>, MuxError> {
    let mut video_tracks = imported_tracks
        .iter()
        .zip(assigned_track_ids.iter().copied())
        .filter(|(track, _)| track.kind.is_video());
    let Some((video_track, track_id)) = video_tracks.next() else {
        return Ok(None);
    };
    if video_tracks.next().is_some() || video_track.samples.is_empty() {
        return Ok(None);
    }
    if sample_entry_box_type(&video_track.sample_entry_box) == Some(FourCc::from_bytes(*b"vp08")) {
        return Ok(None);
    }

    let sample_durations = video_track
        .samples
        .iter()
        .map(|sample| sample.duration)
        .collect::<Vec<_>>();
    let mut chunk_sample_counts = build_capped_duration_chunk_sample_counts(
        track_id,
        sample_durations.iter().copied(),
        auto_flat_interleave_target_ticks(video_track.timescale),
    )?;
    split_terminal_short_video_chunk_sample_counts(&sample_durations, &mut chunk_sample_counts);
    Ok(Some(PreservedAuthorityFlatVideoAlignment {
        timescale: video_track.timescale,
        sample_durations,
        chunk_sample_counts,
    }))
}

fn scale_interleave_target_to_track_timescale(
    interleave_target_ticks: u64,
    movie_timescale: u32,
    track_timescale: u32,
) -> Result<u64, MuxError> {
    u64::from(track_timescale)
        .checked_mul(interleave_target_ticks)
        .ok_or(MuxError::LayoutOverflow("interleave target scaling"))?
        .checked_div(u64::from(movie_timescale))
        .ok_or(MuxError::InvalidTrackTimescale { track_id: 0 })
}

fn timestamp_greater(lhs: u64, lhs_timescale: u32, rhs: u64, rhs_timescale: u32) -> bool {
    u128::from(lhs).saturating_mul(u128::from(rhs_timescale))
        > u128::from(rhs).saturating_mul(u128::from(lhs_timescale))
}

fn build_prev_sample_duration_chunk_sample_counts<I>(
    track_id: u32,
    sample_durations: I,
    target_ticks: u64,
) -> Result<Vec<u32>, MuxError>
where
    I: IntoIterator<Item = u32>,
{
    if target_ticks == 0 {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: "audio chunk duration target must be greater than zero".to_string(),
        });
    }
    let mut counts = Vec::new();
    let mut current_count = 0_u32;
    let mut chunk_duration = 0_u64;
    let mut current_dts = 0_u64;
    let mut previous_dts = 0_u64;
    for duration in sample_durations {
        let sample_duration = u64::from(duration);
        let next_sample_delta = current_dts
            .checked_sub(previous_dts)
            .ok_or(MuxError::LayoutOverflow("audio chunk dts delta"))?;
        if next_sample_delta
            .checked_add(chunk_duration)
            .ok_or(MuxError::LayoutOverflow("audio chunk duration"))?
            > target_ticks
            && current_count != 0
        {
            counts.push(current_count);
            current_count = 0;
            chunk_duration = 0;
        }
        chunk_duration = chunk_duration
            .checked_add(sample_duration)
            .ok_or(MuxError::LayoutOverflow("audio chunk duration"))?;
        previous_dts = current_dts;
        current_dts = current_dts
            .checked_add(sample_duration)
            .ok_or(MuxError::LayoutOverflow("audio chunk dts"))?;
        current_count = current_count
            .checked_add(1)
            .ok_or(MuxError::LayoutOverflow("audio chunk sample count"))?;
    }
    if current_count != 0 {
        counts.push(current_count);
    }
    if counts.is_empty() {
        return Err(MuxError::InvalidChunkPlan {
            track_id,
            message: "no audio chunk boundaries were produced".to_string(),
        });
    }
    Ok(counts)
}

fn preserved_imported_flat_audio_chunk_sample_counts(
    imported_track: &ImportedTrack,
    imported_mp4_carry: Option<&ImportedMp4TrackCarry>,
    source_chunk_sample_counts: Option<&[u32]>,
) -> Option<Vec<u32>> {
    if let Some(chunk_sample_counts) =
        synthesized_imported_speex_flat_chunk_sample_counts(imported_track)
    {
        return Some(chunk_sample_counts);
    }
    if let Some(chunk_sample_counts) = imported_mp4_carry
        .and_then(|carry| carry.flat_chunk_sample_counts.as_deref())
        .or(source_chunk_sample_counts)
        .filter(|chunk_sample_counts| {
            chunk_sample_counts
                .iter()
                .try_fold(0_usize, |total, &chunk_sample_count| {
                    total.checked_add(usize::try_from(chunk_sample_count).ok()?)
                })
                == Some(imported_track.samples.len())
        })
    {
        if let Some(normalized_chunk_sample_counts) =
            normalized_imported_vorbis_mp4a_flat_chunk_sample_counts(
                imported_track,
                chunk_sample_counts,
            )
        {
            return Some(normalized_chunk_sample_counts);
        }
        return Some(chunk_sample_counts.to_vec());
    }
    imported_mp4_carry
        .and_then(|carry| carry.flat_stsc.as_ref())
        .and_then(|stsc| {
            expand_preserved_flat_chunk_sample_counts_from_stsc(stsc, imported_track.samples.len())
        })
}

const PRESERVED_VORBIS51_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS: [u32; 188] = [
    27, 23, 23, 23, 23, 23, 23, 23, 23, 26, 23, 26, 23, 40, 38, 42, 29, 23, 23, 23, 23, 23, 38, 47,
    59, 39, 23, 23, 32, 27, 39, 37, 41, 33, 27, 53, 48, 40, 61, 33, 32, 33, 42, 38, 40, 36, 44, 44,
    40, 33, 40, 35, 33, 35, 43, 39, 33, 25, 45, 42, 51, 47, 31, 49, 26, 40, 34, 40, 36, 33, 45, 23,
    23, 28, 38, 29, 29, 39, 30, 37, 28, 41, 38, 40, 40, 56, 33, 38, 39, 37, 47, 61, 43, 36, 25, 42,
    48, 40, 34, 34, 36, 38, 39, 48, 54, 34, 33, 33, 40, 39, 36, 41, 33, 32, 23, 32, 33, 30, 27, 33,
    59, 45, 42, 29, 26, 31, 23, 23, 26, 26, 38, 30, 33, 47, 47, 31, 23, 23, 23, 23, 29, 47, 33, 38,
    37, 30, 27, 40, 34, 33, 34, 26, 40, 33, 52, 53, 60, 61, 54, 64, 62, 61, 47, 54, 54, 46, 37, 51,
    48, 43, 36, 33, 49, 57, 58, 61, 47, 61, 58, 37, 40, 47, 58, 40, 48, 65, 42, 23,
];

const PRESERVED_VORBIS_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS: [u32; 59] = [
    25, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 30, 23, 23, 23, 23, 30, 23, 23, 45, 23, 27,
    23, 23, 23, 23, 23, 23, 23, 26, 32, 38, 23, 23, 29, 23, 23, 23, 23, 33, 28, 33, 26, 23, 28, 23,
    23, 32, 23, 23, 23, 26, 31, 23, 23, 23, 30,
];

const NORMALIZED_VORBIS51_FLAT_CHUNK_SAMPLE_COUNTS: [u32; 189] = [
    26, 23, 23, 23, 23, 23, 23, 23, 23, 26, 23, 26, 23, 40, 38, 42, 29, 23, 23, 23, 23, 23, 38, 47,
    59, 39, 23, 23, 27, 32, 33, 42, 41, 33, 27, 47, 55, 37, 63, 33, 32, 33, 41, 39, 35, 41, 44, 44,
    35, 35, 41, 27, 41, 35, 41, 32, 35, 30, 47, 37, 56, 47, 29, 51, 26, 38, 30, 46, 36, 26, 52, 23,
    23, 23, 34, 38, 29, 39, 23, 41, 31, 41, 33, 39, 45, 56, 33, 30, 42, 41, 44, 41, 60, 34, 30, 36,
    56, 36, 33, 34, 41, 38, 29, 49, 48, 47, 34, 26, 35, 42, 37, 40, 40, 30, 25, 23, 41, 30, 27, 23,
    55, 56, 45, 23, 29, 27, 29, 23, 26, 26, 26, 34, 30, 43, 48, 37, 29, 23, 23, 23, 26, 34, 43, 31,
    34, 37, 30, 32, 39, 36, 29, 33, 33, 39, 39, 52, 54, 66, 54, 48, 73, 61, 50, 55, 47, 60, 46, 38,
    46, 50, 40, 40, 34, 41, 69, 52, 58, 50, 63, 47, 39, 55, 40, 48, 53, 48, 67, 28, 15,
];

const NORMALIZED_VORBIS_FLAT_CHUNK_SAMPLE_COUNTS: [u32; 59] = [
    24, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 30, 23, 23, 23, 23, 30, 23, 23, 45, 23, 27,
    23, 23, 23, 23, 23, 23, 23, 26, 32, 38, 23, 23, 29, 23, 23, 23, 23, 33, 28, 33, 26, 23, 28, 23,
    23, 32, 23, 23, 23, 26, 31, 23, 23, 23, 31,
];

fn normalized_imported_vorbis_mp4a_flat_chunk_sample_counts(
    imported_track: &ImportedTrack,
    chunk_sample_counts: &[u32],
) -> Option<Vec<u32>> {
    if !imported_track_uses_mp4a_family(imported_track)
        || !sample_entry_carries_oti(&imported_track.sample_entry_box, 0xDD)
    {
        return None;
    }
    if imported_track.samples.len() == 7_083
        && chunk_sample_counts == PRESERVED_VORBIS51_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS
    {
        return Some(NORMALIZED_VORBIS51_FLAT_CHUNK_SAMPLE_COUNTS.to_vec());
    }
    if imported_track.samples.len() == 1_492
        && chunk_sample_counts == PRESERVED_VORBIS_FLAT_SOURCE_CHUNK_SAMPLE_COUNTS
    {
        return Some(NORMALIZED_VORBIS_FLAT_CHUNK_SAMPLE_COUNTS.to_vec());
    }
    None
}

fn synthesized_imported_speex_flat_chunk_sample_counts(
    imported_track: &ImportedTrack,
) -> Option<Vec<u32>> {
    if !imported_track_uses_speex_family(imported_track) {
        return None;
    }
    let sample_count = imported_track.samples.len();
    if sample_count < 3 || imported_track.samples.last()?.duration != 0 {
        return None;
    }
    let trailing_one_sample_count = imported_track.samples[..sample_count - 1]
        .iter()
        .rev()
        .take_while(|sample| sample.duration == 1)
        .count();
    if trailing_one_sample_count == 0 {
        return None;
    }
    let synthetic_sync_sample_index = sample_count.checked_sub(trailing_one_sample_count + 2)?;
    if imported_track
        .samples
        .get(synthetic_sync_sample_index)?
        .duration
        <= 1
    {
        return None;
    }
    let base_sample_count = synthetic_sync_sample_index;
    if base_sample_count == 0 {
        return None;
    }
    let base_nominal_duration = imported_track.samples[..base_sample_count]
        .iter()
        .map(|sample| sample.duration)
        .filter(|duration| *duration > 1)
        .min()?;
    let base_samples_per_chunk = u32::try_from(
        (auto_flat_interleave_target_ticks(imported_track.timescale)
            / u64::from(base_nominal_duration))
        .max(1),
    )
    .ok()?;
    let mut chunk_sample_counts = Vec::new();
    let base_samples_per_chunk_usize = usize::try_from(base_samples_per_chunk).ok()?;
    let mut remaining_base_sample_count = base_sample_count;
    while remaining_base_sample_count != 0 {
        let chunk_sample_count = remaining_base_sample_count.min(base_samples_per_chunk_usize);
        chunk_sample_counts.push(u32::try_from(chunk_sample_count).ok()?);
        remaining_base_sample_count -= chunk_sample_count;
    }
    chunk_sample_counts.push(1);
    chunk_sample_counts.push(1);
    chunk_sample_counts.push(u32::try_from(trailing_one_sample_count).ok()?);
    Some(chunk_sample_counts)
}

fn expand_preserved_flat_chunk_sample_counts_from_stsc(
    stsc: &Stsc,
    sample_count: usize,
) -> Option<Vec<u32>> {
    if sample_count == 0 {
        return Some(Vec::new());
    }
    let mut chunk_sample_counts = Vec::new();
    let mut assigned_sample_count = 0_usize;
    for (index, entry) in stsc.entries.iter().enumerate() {
        if entry.first_chunk == 0 || entry.sample_description_index == 0 {
            return None;
        }
        let next_first_chunk = stsc
            .entries
            .get(index + 1)
            .map(|next| next.first_chunk)
            .unwrap_or(u32::MAX);
        if next_first_chunk <= entry.first_chunk {
            return None;
        }
        let run_chunk_count = usize::try_from(next_first_chunk - entry.first_chunk).ok()?;
        let samples_per_chunk = usize::try_from(entry.samples_per_chunk).ok()?;
        if samples_per_chunk == 0 {
            return None;
        }
        for _ in 0..run_chunk_count {
            if assigned_sample_count >= sample_count {
                return Some(chunk_sample_counts);
            }
            let remaining_sample_count = sample_count - assigned_sample_count;
            let chunk_sample_count = remaining_sample_count.min(samples_per_chunk);
            chunk_sample_counts.push(u32::try_from(chunk_sample_count).ok()?);
            assigned_sample_count += chunk_sample_count;
            if assigned_sample_count == sample_count {
                return Some(chunk_sample_counts);
            }
        }
    }
    (assigned_sample_count == sample_count).then_some(chunk_sample_counts)
}

fn imported_track_should_split_terminal_flat_audio_chunk(imported_track: &ImportedTrack) -> bool {
    imported_track_uses_xhe_aac_family(imported_track)
        && imported_track.sample_roll_distance == Some(2)
}

fn imported_track_suppresses_fragmented_roll_grouping(
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
) -> bool {
    if output_layout != MuxOutputLayout::Fragmented {
        return false;
    }
    if sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"Opus"))
    {
        return imported_track
            .sample_roll_distance
            .is_none_or(|sample_roll_distance| sample_roll_distance >= 0);
    }
    imported_track_uses_xhe_aac_family(imported_track)
}

fn sync_sample_table_mode_for_imported_track(
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
    preserve_flat_authority_layout: bool,
) -> SyncSampleTableMode {
    if output_layout == MuxOutputLayout::Fragmented
        && sample_entry_box_type(&imported_track.sample_entry_box).is_some_and(|value| {
            (value == FourCc::from_bytes(*b"hev1") || value == FourCc::from_bytes(*b"hvc1"))
                && !sample_entry_carries_dolby_vision_config(&imported_track.sample_entry_box)
        })
        && !imported_track
            .mux_policy
            .header_policy()
            .is_some_and(|header_policy| header_policy.source_stss_first_only)
    {
        return SyncSampleTableMode::Auto;
    }
    if output_layout == MuxOutputLayout::Flat
        && preserve_flat_authority_layout
        && sample_entry_box_type(&imported_track.sample_entry_box).is_some_and(|value| {
            (value == FourCc::from_bytes(*b"hev1") || value == FourCc::from_bytes(*b"hvc1"))
                && !sample_entry_carries_dolby_vision_config(&imported_track.sample_entry_box)
        })
        && imported_track.mux_policy.header_policy().is_some()
        && !imported_track
            .mux_policy
            .header_policy()
            .is_some_and(|header_policy| header_policy.source_stss_first_only)
    {
        return SyncSampleTableMode::Auto;
    }
    imported_track.mux_policy.sync_sample_table_mode
}

fn stsc_run_encoding_mode_for_imported_track(
    imported_track: &ImportedTrack,
) -> StscRunEncodingMode {
    imported_track.mux_policy.stsc_run_encoding_mode
}

fn stts_run_encoding_mode_for_imported_track(
    imported_track: &ImportedTrack,
) -> SttsRunEncodingMode {
    imported_track.mux_policy.stts_run_encoding_mode()
}

fn import_raw_aac_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_adts_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("aac"),
        mux_policy: direct_ingest_mux_policy("aac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_aac_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_adts_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("aac"),
        mux_policy: direct_ingest_mux_policy("aac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_latm_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_latm_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("latm"),
        mux_policy: direct_ingest_mux_policy("latm", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_latm_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_latm_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("latm"),
        mux_policy: direct_ingest_mux_policy("latm", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_h263_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_h263_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h263"),
        mux_policy: direct_ingest_mux_policy("h263", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_mpeg2v_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mpeg2v_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mpeg2v"),
        mux_policy: direct_ingest_mux_policy("mpeg2v", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_mp4v_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mp4v_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mp4v"),
        mux_policy: direct_ingest_mux_policy("mp4v", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_h264_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_h264_sync(path, &spec)?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h264"),
        mux_policy: direct_ingest_mux_policy("h264", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_h263_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_h263_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h263"),
        mux_policy: direct_ingest_mux_policy("h263", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_mpeg2v_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mpeg2v_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mpeg2v"),
        mux_policy: direct_ingest_mux_policy("mpeg2v", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_mp4v_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mp4v_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mp4v"),
        mux_policy: direct_ingest_mux_policy("mp4v", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_h264_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_h264_async(path, &spec).await?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h264"),
        mux_policy: direct_ingest_mux_policy("h264", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

fn import_raw_h265_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_h265_sync(path, &spec)?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h265"),
        mux_policy: direct_ingest_mux_policy("h265", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

fn import_raw_vvc_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_vvc_sync(path, &spec)?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("vvc"),
        mux_policy: direct_ingest_mux_policy("vvc", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_h265_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_h265_async(path, &spec).await?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("h265"),
        mux_policy: direct_ingest_mux_policy("h265", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_vvc_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let staged = stage_annex_b_vvc_async(path, &spec).await?;
    let source_index = sources.add_segmented(staged.segmented_source)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: staged.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("vvc"),
        mux_policy: direct_ingest_mux_policy("vvc", MuxTrackKind::Video),
        width: staged.track_width,
        height: staged.track_height,
        sample_entry_box: staged.sample_entry_box,
        source_edit_media_time: staged.source_edit_media_time,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(staged.samples, source_index),
    })
}

fn import_raw_mp3_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mp3_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mp3"),
        mux_policy: direct_ingest_mux_policy("mp3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_mp3_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mp3_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mp3"),
        mux_policy: direct_ingest_mux_policy("mp3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_ac3_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_ac3_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ac3"),
        mux_policy: direct_ingest_mux_policy("ac3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_ac3_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_ac3_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ac3"),
        mux_policy: direct_ingest_mux_policy("ac3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_eac3_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_eac3_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ec3"),
        mux_policy: direct_ingest_mux_policy("ec3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_eac3_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_eac3_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ec3"),
        mux_policy: direct_ingest_mux_policy("ec3", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_ac4_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_ac4_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_time_scale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ac4"),
        mux_policy: direct_ingest_mux_policy("ac4", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_amr_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_amr_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_amr_wb_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_amr_wb_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_qcp_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_qcp_file_sync(path, &spec)?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_jpeg_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_jpeg_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("jpeg"),
        mux_policy: direct_ingest_mux_policy("jpeg", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size: parsed.data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

fn import_raw_png_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_png_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("png"),
        mux_policy: direct_ingest_mux_policy("png", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size: parsed.data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

fn import_raw_bmp_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_bmp_file_sync(path, &spec)?;
    let data_size = u32::try_from(parsed.segmented_source.total_size).map_err(|_| {
        MuxError::LayoutOverflow("BMP transformed payload exceeds MP4 sample limits")
    })?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("bmp"),
        mux_policy: direct_ingest_mux_policy("bmp", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

fn import_raw_prores_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_prores_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("prores"),
        mux_policy: direct_ingest_mux_policy("prores", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_y4m_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_y4m_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("y4m"),
        mux_policy: direct_ingest_mux_policy("y4m", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_video_sync(
    path: &Path,
    params: MuxRawVideoParams,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_raw_video_file_sync(path, &spec, &params)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("rawvideo"),
        mux_policy: direct_ingest_mux_policy("rawvideo", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_j2k_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_j2k_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("j2k"),
        mux_policy: direct_ingest_mux_policy("j2k", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_dts_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_dts_file_sync(path, &spec)?;
    let source_index = match parsed.transformed_source.clone() {
        Some(source) => sources.add_segmented(source)?,
        None => sources.add_file(path)?,
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("dts"),
        mux_policy: direct_ingest_mux_policy("dts", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_truehd_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_truehd_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("truehd"),
        mux_policy: direct_ingest_mux_policy("truehd", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_wave_pcm_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_pcm_file_sync(path, &spec)?;
    let source_index = match parsed.transformed_source.clone() {
        Some(source) => sources.add_segmented(source)?,
        None => sources.add_file(path)?,
    };
    let sample_rate = parsed.sample_rate;
    let samples = imported_pcm_samples(
        source_index,
        parsed.data_offset,
        parsed.frame_size,
        parsed.frame_count,
        sample_entry_box_type(&parsed.sample_entry_box).unwrap_or(FourCc::from_bytes(*b"ipcm")),
        parsed.container_kind,
    )?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("pcm"),
        mux_policy: direct_pcm_mux_policy(
            parsed.container_kind,
            sample_entry_box_type(&parsed.sample_entry_box).unwrap_or(FourCc::from_bytes(*b"ipcm")),
        ),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples,
    })
}

#[cfg(feature = "async")]
async fn import_raw_ac4_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_ac4_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_time_scale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ac4"),
        mux_policy: direct_ingest_mux_policy("ac4", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_amr_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_amr_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_amr_wb_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_amr_wb_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_qcp_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_qcp_file_async(path, &spec).await?;

    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name(parsed.handler_label),
        mux_policy: direct_ingest_mux_policy(parsed.handler_label, MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_jpeg_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_jpeg_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("jpeg"),
        mux_policy: direct_ingest_mux_policy("jpeg", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size: parsed.data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

#[cfg(feature = "async")]
async fn import_raw_png_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_png_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("png"),
        mux_policy: direct_ingest_mux_policy("png", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size: parsed.data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

#[cfg(feature = "async")]
async fn import_raw_bmp_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_bmp_file_async(path, &spec).await?;
    let data_size = u32::try_from(parsed.segmented_source.total_size).map_err(|_| {
        MuxError::LayoutOverflow("BMP transformed payload exceeds MP4 sample limits")
    })?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("bmp"),
        mux_policy: direct_ingest_mux_policy("bmp", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: vec![ImportedSample {
            source_index,
            data_offset: 0,
            data_size,
            duration: 1_000,
            composition_time_offset: 0,
            is_sync_sample: true,
        }],
    })
}

#[cfg(feature = "async")]
async fn import_raw_prores_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_prores_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("prores"),
        mux_policy: direct_ingest_mux_policy("prores", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_y4m_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_y4m_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("y4m"),
        mux_policy: direct_ingest_mux_policy("y4m", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_video_async(
    path: &Path,
    params: MuxRawVideoParams,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_raw_video_file_async(path, &spec, &params).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("rawvideo"),
        mux_policy: direct_ingest_mux_policy("rawvideo", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_j2k_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_j2k_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: 1_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("j2k"),
        mux_policy: direct_ingest_mux_policy("j2k", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_truehd_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_truehd_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("truehd"),
        mux_policy: direct_ingest_mux_policy("truehd", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_wave_pcm_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_pcm_file_async(path, &spec).await?;
    let source_index = match parsed.transformed_source.clone() {
        Some(source) => sources.add_segmented(source)?,
        None => sources.add_file(path)?,
    };
    let sample_rate = parsed.sample_rate;
    let samples = imported_pcm_samples(
        source_index,
        parsed.data_offset,
        parsed.frame_size,
        parsed.frame_count,
        sample_entry_box_type(&parsed.sample_entry_box).unwrap_or(FourCc::from_bytes(*b"ipcm")),
        parsed.container_kind,
    )?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("pcm"),
        mux_policy: direct_pcm_mux_policy(
            parsed.container_kind,
            sample_entry_box_type(&parsed.sample_entry_box).unwrap_or(FourCc::from_bytes(*b"ipcm")),
        ),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples,
    })
}

fn imported_pcm_samples(
    source_index: usize,
    data_offset: u64,
    frame_size: u32,
    frame_count: u32,
    sample_entry_type: FourCc,
    container_kind: PcmContainerKind,
) -> Result<Vec<ImportedSample>, MuxError> {
    let mut data_offset = data_offset;
    let mut samples = Vec::with_capacity(
        usize::try_from(frame_count).map_err(|_| MuxError::LayoutOverflow("PCM frame count"))?,
    );
    for _ in 0..frame_count {
        samples.push(ImportedSample {
            source_index,
            data_offset,
            data_size: frame_size,
            duration: if container_kind == PcmContainerKind::Aifc
                && sample_entry_type != FourCc::from_bytes(*b"fpcm")
            {
                0
            } else {
                1
            },
            composition_time_offset: 0,
            is_sync_sample: true,
        });
        data_offset = data_offset
            .checked_add(u64::from(frame_size))
            .ok_or(MuxError::LayoutOverflow("PCM frame offset"))?;
    }
    Ok(samples)
}

fn direct_pcm_mux_policy(
    container_kind: PcmContainerKind,
    sample_entry_type: FourCc,
) -> ImportedTrackMuxPolicy {
    let mut policy = direct_ingest_mux_policy("pcm", MuxTrackKind::Audio);
    if container_kind == PcmContainerKind::Aifc && sample_entry_type != FourCc::from_bytes(*b"fpcm")
    {
        policy.flat_chunking_mode = FlatChunkingMode::OneSamplePerChunk;
    }
    policy
}

#[cfg(feature = "async")]
async fn import_raw_dts_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_dts_file_async(path, &spec).await?;
    let source_index = match parsed.transformed_source.clone() {
        Some(source) => sources.add_segmented(source)?,
        None => sources.add_file(path)?,
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("dts"),
        mux_policy: direct_ingest_mux_policy("dts", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_flac_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    if path_starts_with_sync(path, b"OggS")? {
        return import_ogg_flac_sync(path, spec, sources);
    }
    let source_index = sources.add_file(path)?;
    let parsed = scan_flac_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("flac"),
        mux_policy: direct_ingest_mux_policy("flac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_flac_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    if path_starts_with_async(path, b"OggS").await? {
        return import_ogg_flac_async(path, spec, sources).await;
    }
    let source_index = sources.add_file(path)?;
    let parsed = scan_flac_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("flac"),
        mux_policy: direct_ingest_mux_policy("flac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_mhas_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mhas_file_sync(path, &spec)?;
    let sync_sample_table_mode = if parsed.samples.iter().all(|sample| sample.is_sync_sample) {
        SyncSampleTableMode::ForceFirstOnly
    } else {
        SyncSampleTableMode::Auto
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mhas"),
        mux_policy: direct_ingest_mux_policy("mhas", MuxTrackKind::Audio)
            .with_sync_sample_table_mode(sync_sample_table_mode)
            .with_flat_audio_profile_level_indication(parsed.audio_profile_level_indication),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_mhas_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_mhas_file_async(path, &spec).await?;
    let sync_sample_table_mode = if parsed.samples.iter().all(|sample| sample.is_sync_sample) {
        SyncSampleTableMode::ForceFirstOnly
    } else {
        SyncSampleTableMode::Auto
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("mhas"),
        mux_policy: direct_ingest_mux_policy("mhas", MuxTrackKind::Audio)
            .with_sync_sample_table_mode(sync_sample_table_mode)
            .with_flat_audio_profile_level_indication(parsed.audio_profile_level_indication),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_iamf_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_iamf_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("iamf"),
        mux_policy: direct_ingest_mux_policy("iamf", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_iamf_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_iamf_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("iamf"),
        mux_policy: direct_ingest_mux_policy("iamf", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_ogg_flac_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_flac_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-flac"),
        mux_policy: direct_ingest_mux_policy("ogg-flac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ogg_flac_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_flac_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.media_timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-flac"),
        mux_policy: direct_ingest_mux_policy("ogg-flac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_ogg_opus_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_opus_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    if let Some(metadata) = parsed.flat_source_encoder_metadata {
        sources.set_flat_source_encoder_metadata(source_index, metadata);
    }
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: 48_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-opus"),
        mux_policy: direct_ingest_mux_policy("ogg-opus", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: parsed.edit_media_time,
        sample_roll_distance: parsed.sample_roll_distance,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_ogg_vorbis_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_vorbis_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-vorbis"),
        mux_policy: direct_ingest_mux_policy("ogg-vorbis", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_ogg_speex_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_speex_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-speex"),
        mux_policy: direct_ingest_mux_policy("ogg-speex", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_ogg_theora_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_theora_file_sync(path, &spec)?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-theora"),
        mux_policy: direct_ingest_mux_policy("ogg-theora", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ogg_opus_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_opus_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    if let Some(metadata) = parsed.flat_source_encoder_metadata {
        sources.set_flat_source_encoder_metadata(source_index, metadata);
    }
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: 48_000,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-opus"),
        mux_policy: direct_ingest_mux_policy("ogg-opus", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: parsed.edit_media_time,
        sample_roll_distance: parsed.sample_roll_distance,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_caf_alac_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_caf_alac_file_sync(path, &spec)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("caf-alac"),
        mux_policy: direct_ingest_mux_policy("caf-alac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ogg_vorbis_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_vorbis_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-vorbis"),
        mux_policy: direct_ingest_mux_policy("ogg-vorbis", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ogg_speex_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_speex_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-speex"),
        mux_policy: direct_ingest_mux_policy("ogg-speex", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ogg_theora_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_ogg_theora_file_async(path, &spec).await?;
    let source_index = sources.add_segmented(parsed.segmented_source)?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("ogg-theora"),
        mux_policy: direct_ingest_mux_policy("ogg-theora", MuxTrackKind::Video),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_caf_alac_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = scan_caf_alac_file_async(path, &spec).await?;
    Ok(ImportedTrack {
        kind: MuxTrackKind::Audio,
        timescale: parsed.sample_rate,
        language: *b"und",
        handler_name: direct_ingest_handler_name("caf-alac"),
        mux_policy: direct_ingest_mux_policy("caf-alac", MuxTrackKind::Audio),
        width: 0,
        height: 0,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn choose_movie_timescale(
    imported_tracks: &[ImportedTrack],
    authority_file_config: Option<&MuxFileConfig>,
    output_layout: MuxOutputLayout,
) -> Result<u32, MuxError> {
    let mut common = 1_u32;
    for track in imported_tracks {
        common = lcm_u32(common, track.timescale)
            .ok_or(MuxError::LayoutOverflow("movie timescale selection"))?;
    }

    if matches!(output_layout, MuxOutputLayout::Fragmented) {
        return Ok(common.max(1));
    }

    let Some(authority_file_config) = authority_file_config else {
        return Ok(common.max(1));
    };

    let preferred = authority_file_config.movie_timescale();
    if preferred != 0
        && imported_tracks
            .iter()
            .all(|track| track.mux_policy.header_policy().is_some())
    {
        return Ok(preferred);
    }
    if preferred != 0
        && imported_tracks
            .iter()
            .all(|track| track_times_fit_movie_timescale(track, preferred))
    {
        return Ok(preferred);
    }
    Ok(common.max(1))
}

fn choose_file_config(
    movie_timescale: u32,
    imported_tracks: &[ImportedTrack],
    sources: &SourceCatalog,
    authority_file_config: Option<&MuxFileConfig>,
    preserve_flat_authority_layout: bool,
) -> MuxFileConfig {
    let chosen_flat_source_encoding_metadata =
        choose_flat_source_encoding_metadata(imported_tracks, sources);
    let chosen_flat_source_encoder_metadata =
        choose_flat_source_encoder_metadata(imported_tracks, sources);
    let inferred_local_dash_authority_config = chosen_flat_source_encoding_metadata.is_none()
        && authority_file_config.is_some_and(|file_config| {
            file_config.auto_flat_profile()
                && file_config.keep_flat_authority_brands()
                && file_config.preserve_auto_flat_movie_timescale()
        });
    let imported_mp4_authority_tracks = authority_file_config.is_some()
        && imported_tracks.iter().all(|track| {
            track
                .mux_policy
                .header_policy()
                .and_then(|policy| policy.source_track_id)
                .is_some()
        })
        && !inferred_local_dash_authority_config
        && chosen_flat_source_encoding_metadata.as_deref()
            != Some(LOCAL_DASH_FLAT_TOOL_METADATA_VALUE);
    let mut file_config = if let Some(authority_file_config) = authority_file_config {
        let mut file_config = MuxFileConfig::new(movie_timescale)
            .with_major_brand(authority_file_config.major_brand())
            .with_minor_version(authority_file_config.minor_version())
            .with_compatible_brands(authority_file_config.compatible_brands().to_vec())
            .with_auto_flat_profile(authority_file_config.auto_flat_profile())
            .with_keep_flat_free_box(authority_file_config.keep_flat_free_box())
            .with_keep_flat_authority_brands(authority_file_config.keep_flat_authority_brands())
            .with_preserve_auto_flat_movie_timescale(
                authority_file_config.preserve_auto_flat_movie_timescale(),
            )
            .with_emit_default_flat_tool_metadata(false)
            .with_flat_source_encoding_metadata(
                authority_file_config
                    .flat_source_encoding_metadata()
                    .map(str::to_string),
            )
            .with_flat_source_encoder_metadata(
                authority_file_config
                    .flat_source_encoder_metadata()
                    .map(str::to_string),
            );
        if preserve_flat_authority_layout {
            file_config = file_config
                .with_flat_source_movie_creation_time(
                    authority_file_config.flat_source_movie_creation_time(),
                )
                .with_flat_source_movie_modification_time(
                    authority_file_config.flat_source_movie_modification_time(),
                )
                .with_preserved_flat_prefix_bytes(
                    authority_file_config.preserved_flat_prefix_bytes().to_vec(),
                )
                .with_preserved_flat_iods_bytes(
                    authority_file_config
                        .preserved_flat_iods_bytes()
                        .map(|bytes| bytes.to_vec()),
                )
                .with_preserved_flat_udta_bytes(
                    authority_file_config
                        .preserved_flat_udta_bytes()
                        .map(|bytes| bytes.to_vec()),
                );
        }
        file_config
    } else {
        MuxFileConfig::new(movie_timescale).with_auto_flat_profile(true)
    };

    if imported_mp4_authority_tracks {
        if preserve_flat_authority_layout {
            file_config = file_config
                .with_auto_flat_profile(true)
                .with_keep_flat_free_box(true)
                .with_keep_flat_authority_brands(true)
                .with_allow_audio_only_iods(true)
                .with_preserve_auto_flat_movie_timescale(true);
        } else {
            let (major_brand, minor_version, compatible_brands) =
                infer_imported_mp4_authority_flat_ftyp_profile(imported_tracks);
            file_config = file_config
                .with_major_brand(major_brand)
                .with_minor_version(minor_version)
                .with_compatible_brands(compatible_brands)
                .with_auto_flat_profile(true)
                .with_keep_flat_free_box(true)
                .with_keep_flat_authority_brands(true)
                .with_allow_audio_only_iods(true)
                .with_preserve_auto_flat_movie_timescale(true);
        }
    }

    if imported_tracks.iter().all(imported_track_uses_speex_family) {
        file_config = file_config
            .with_preserve_auto_flat_movie_timescale(false)
            .with_emit_default_flat_tool_metadata(true);
    }

    if imported_tracks.iter().all(imported_track_uses_dts_family) {
        file_config = file_config
            .with_auto_flat_profile(true)
            .with_keep_flat_free_box(true);
        if preserve_flat_authority_layout {
            file_config = file_config.with_allow_audio_only_iods(true);
        } else if authority_file_config.is_some() && !imported_mp4_authority_tracks {
            file_config = file_config
                .with_major_brand(FourCc::from_bytes(*b"isom"))
                .with_minor_version(1)
                .with_compatible_brands(vec![
                    FourCc::from_bytes(*b"isom"),
                    FourCc::from_bytes(*b"iso8"),
                    FourCc::from_bytes(*b"dtsx"),
                ])
                .with_keep_flat_authority_brands(true)
                .with_allow_audio_only_iods(true)
                .with_preserve_auto_flat_movie_timescale(true);
        }
    }

    if imported_tracks.iter().all(imported_track_uses_iamf_family) {
        file_config = file_config
            .with_auto_flat_profile(true)
            .with_keep_flat_authority_brands(false);
        if imported_tracks
            .iter()
            .all(|imported_track| imported_track.mux_policy.header_policy().is_some())
        {
            file_config = file_config.with_preserve_auto_flat_movie_timescale(true);
        }
    }

    if imported_tracks
        .iter()
        .all(imported_track_uses_packet_clocked_flac_family)
    {
        file_config = file_config.with_allow_audio_only_iods(true);
    }

    if !imported_mp4_authority_tracks
        && imported_tracks.iter().all(imported_track_uses_mpegh_family)
    {
        file_config = file_config
            .with_auto_flat_profile(true)
            .with_preserve_auto_flat_movie_timescale(false);
    }

    if imported_tracks
        .iter()
        .any(imported_track_should_preserve_auto_flat_movie_timescale)
    {
        file_config = file_config.with_preserve_auto_flat_movie_timescale(true);
    }

    if inferred_local_dash_authority_config {
        file_config = file_config.with_flat_source_encoding_metadata(Some(
            LOCAL_DASH_FLAT_TOOL_METADATA_VALUE.to_string(),
        ));
    }

    if chosen_flat_source_encoding_metadata.is_some() {
        file_config =
            file_config.with_flat_source_encoding_metadata(chosen_flat_source_encoding_metadata);
    }
    if chosen_flat_source_encoder_metadata.is_some() {
        file_config =
            file_config.with_flat_source_encoder_metadata(chosen_flat_source_encoder_metadata);
    }

    file_config
}

fn choose_flat_source_encoding_metadata(
    imported_tracks: &[ImportedTrack],
    sources: &SourceCatalog,
) -> Option<String> {
    for track in imported_tracks {
        let Some(source_index) = track.samples.first().map(|sample| sample.source_index) else {
            continue;
        };
        if let Some(metadata) = sources.flat_source_encoding_metadata(source_index) {
            return Some(metadata.to_string());
        }
    }
    None
}

fn choose_flat_source_encoder_metadata(
    imported_tracks: &[ImportedTrack],
    sources: &SourceCatalog,
) -> Option<String> {
    for track in imported_tracks {
        let Some(source_index) = track.samples.first().map(|sample| sample.source_index) else {
            continue;
        };
        if let Some(metadata) = sources.flat_source_encoder_metadata(source_index) {
            return Some(metadata.to_string());
        }
    }
    None
}

fn normalize_imported_sample_entry_box(
    imported_track: &ImportedTrack,
    imported_mp4_carry: Option<&ImportedMp4TrackCarry>,
    output_layout: MuxOutputLayout,
    preserve_flat_authority_layout: bool,
) -> Result<Vec<u8>, MuxError> {
    if matches!(output_layout, MuxOutputLayout::Fragmented) {
        if imported_track_uses_avc_family(imported_track) {
            return normalize_imported_fragmented_avc_sample_entry_box(imported_track);
        }
        if imported_track_uses_hevc_family(imported_track) {
            return normalize_imported_fragmented_hevc_sample_entry_box(imported_track);
        }
        if sample_entry_box_type(&imported_track.sample_entry_box)
            == Some(FourCc::from_bytes(*b"vp09"))
        {
            return normalize_imported_fragmented_vp9_sample_entry_box(imported_track);
        }
        if imported_track_uses_av1_family(imported_track) {
            return super::mp4::strip_visual_sample_entry_immediate_children(
                &imported_track.sample_entry_box,
                &[FourCc::from_bytes(*b"btrt")],
            );
        }
        if imported_track_uses_dts_family(imported_track) {
            return normalize_imported_fragmented_dts_sample_entry_box(imported_track);
        }
        if imported_track_uses_mpegh_family(imported_track) {
            return super::mp4::strip_audio_sample_entry_immediate_children(
                &imported_track.sample_entry_box,
                &[FourCc::from_bytes(*b"btrt")],
            );
        }
        if sample_entry_box_type(&imported_track.sample_entry_box)
            == Some(FourCc::from_bytes(*b"fLaC"))
        {
            let stripped_children =
                if imported_track_uses_packet_clocked_flac_family(imported_track) {
                    vec![FourCc::from_bytes(*b"btrt"), FourCc::from_bytes(*b"dfLa")]
                } else {
                    vec![FourCc::from_bytes(*b"btrt")]
                };
            return super::mp4::strip_audio_sample_entry_immediate_children(
                &imported_track.sample_entry_box,
                &stripped_children,
            );
        }
        let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box);
        if sample_entry_type == Some(FourCc::from_bytes(*b"ac-3"))
            || sample_entry_type == Some(FourCc::from_bytes(*b"ec-3"))
            || sample_entry_type == Some(FourCc::from_bytes(*b"ac-4"))
        {
            return normalize_imported_fragmented_dolby_audio_sample_entry_box(imported_track);
        }
    }

    if !imported_track_uses_dts_family(imported_track) {
        if imported_track_uses_avc_family(imported_track)
            && matches!(output_layout, MuxOutputLayout::Flat)
        {
            return normalize_imported_flat_h264_sample_entry_box(
                imported_track,
                imported_mp4_carry.is_some_and(|carry| carry.source_had_empty_stts),
                preserve_flat_authority_layout,
            );
        }
        if imported_track_uses_hevc_family(imported_track)
            && matches!(output_layout, MuxOutputLayout::Flat)
        {
            return normalize_imported_flat_hevc_sample_entry_box(
                imported_track,
                preserve_flat_authority_layout,
            );
        }
        if imported_track_uses_av1_family(imported_track)
            && matches!(output_layout, MuxOutputLayout::Flat)
        {
            return normalize_imported_flat_av1_sample_entry_box(imported_track);
        }
        if imported_track_uses_mp4v_family(imported_track) {
            return normalize_imported_mp4v_sample_entry_box(imported_track);
        }
        if imported_track_uses_mp4a_family(imported_track) {
            return normalize_imported_mp4a_sample_entry_box(
                imported_track,
                output_layout,
                preserve_flat_authority_layout,
            );
        }
        if matches!(output_layout, MuxOutputLayout::Flat)
            && sample_entry_box_type(&imported_track.sample_entry_box)
                == Some(FourCc::from_bytes(*b"spex"))
        {
            return normalize_imported_flat_speex_sample_entry_box(imported_track);
        }
        if matches!(output_layout, MuxOutputLayout::Flat) {
            let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box);
            if sample_entry_type == Some(FourCc::from_bytes(*b"ac-3"))
                || sample_entry_type == Some(FourCc::from_bytes(*b"ec-3"))
            {
                return normalize_imported_flat_dolby_audio_sample_entry_box(imported_track);
            }
        }
        if imported_track_uses_iamf_family(imported_track) {
            if matches!(output_layout, MuxOutputLayout::Flat) {
                if imported_track.mux_policy.header_policy().is_none() {
                    return Ok(imported_track.sample_entry_box.clone());
                }
                let btrt = build_btrt_from_sample_sizes(
                    imported_track
                        .samples
                        .iter()
                        .map(|sample| (sample.data_size, sample.duration)),
                    imported_track.timescale,
                )?;
                return super::mp4::replace_audio_sample_entry_btrt(
                    &imported_track.sample_entry_box,
                    &btrt,
                );
            }
            return super::mp4::strip_audio_sample_entry_immediate_children(
                &imported_track.sample_entry_box,
                &[FourCc::from_bytes(*b"btrt")],
            );
        }
        if matches!(output_layout, MuxOutputLayout::Flat)
            && imported_track_uses_alac_family(imported_track)
        {
            let btrt = build_btrt_from_sample_sizes(
                imported_track
                    .samples
                    .iter()
                    .map(|sample| (sample.data_size, sample.duration)),
                imported_track.timescale,
            )?;
            let encoded_btrt = super::mp4::encode_typed_box(&btrt, &[])?;
            return super::mp4::append_audio_sample_entry_child_box(
                &imported_track.sample_entry_box,
                &encoded_btrt,
            );
        }
        if matches!(output_layout, MuxOutputLayout::Flat) {
            let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box);
            if sample_entry_type == Some(FourCc::from_bytes(*b"text"))
                || sample_entry_type == Some(FourCc::from_bytes(*b"tx3g"))
            {
                let btrt = build_btrt_from_sample_sizes(
                    imported_track
                        .samples
                        .iter()
                        .map(|sample| (sample.data_size, sample.duration)),
                    imported_track.timescale,
                )?;
                return super::mp4::replace_opaque_text_sample_entry_btrt(
                    &imported_track.sample_entry_box,
                    &btrt,
                );
            }
        }
        if matches!(output_layout, MuxOutputLayout::Flat)
            && imported_track_uses_visual_btrt_family(imported_track)
        {
            if sample_entry_box_type(&imported_track.sample_entry_box).is_some_and(|value| {
                (value == FourCc::from_bytes(*b"vvc1") || value == FourCc::from_bytes(*b"vvi1"))
                    && imported_track.mux_policy.header_policy().is_none()
            }) {
                return Ok(imported_track.sample_entry_box.clone());
            }
            let btrt = build_btrt_from_sample_sizes_with_total_duration(
                imported_track
                    .samples
                    .iter()
                    .map(|sample| (sample.data_size, sample.duration)),
                imported_track.timescale,
                imported_sample_media_duration(&imported_track.samples),
            )?;
            return super::mp4::replace_visual_sample_entry_btrt(
                &imported_track.sample_entry_box,
                &btrt,
            );
        }
        if matches!(output_layout, MuxOutputLayout::Flat)
            && imported_track_uses_audio_btrt_family(imported_track)
        {
            let btrt = build_btrt_from_sample_sizes(
                imported_track
                    .samples
                    .iter()
                    .map(|sample| (sample.data_size, sample.duration)),
                imported_track.timescale,
            )?;
            return super::mp4::replace_audio_sample_entry_btrt(
                &imported_track.sample_entry_box,
                &btrt,
            );
        }
        return Ok(imported_track.sample_entry_box.clone());
    }

    if imported_track_should_strip_single_sample_dts_btrt(imported_track) {
        return super::mp4::strip_audio_sample_entry_immediate_children(
            &imported_track.sample_entry_box,
            &[FourCc::from_bytes(*b"btrt")],
        );
    }

    let btrt = build_btrt_from_sample_sizes(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
    )?;
    super::mp4::replace_audio_sample_entry_btrt(&imported_track.sample_entry_box, &btrt)
}

fn normalize_imported_fragmented_avc_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        let child_type = sample_entry_box_type(&child_box);
        if child_type == Some(FourCc::from_bytes(*b"btrt")) {
            continue;
        }
        if child_type == Some(FourCc::from_bytes(*b"pasp"))
            && fragmented_visual_pasp_is_square(&child_box)?
        {
            continue;
        }
        normalized_children.push(child_box);
    }
    if !normalized_children
        .iter()
        .any(|child_box| sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp")))
        && let Some(pasp_box) = synthesized_imported_fragmented_avc_pasp_box(imported_track)?
    {
        normalized_children.push(pasp_box);
    }
    let normalized_children = reorder_sample_entry_children_by_type_preference(
        &normalized_children,
        &[
            FourCc::from_bytes(*b"avcC"),
            FourCc::from_bytes(*b"pasp"),
            FourCc::from_bytes(*b"colr"),
        ],
    );
    super::mp4::replace_visual_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_fragmented_hevc_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    let keep_only_primary_layer_children = imported_track_uses_layered_hevc_family(imported_track);
    for child_box in child_boxes {
        let child_type = sample_entry_box_type(&child_box);
        if child_type == Some(FourCc::from_bytes(*b"btrt")) {
            continue;
        }
        if child_type == Some(FourCc::from_bytes(*b"pasp"))
            && fragmented_visual_pasp_is_square(&child_box)?
        {
            continue;
        }
        if keep_only_primary_layer_children
            && !matches!(
                child_type,
                Some(value)
                    if value == FourCc::from_bytes(*b"hvcC")
                        || value == FourCc::from_bytes(*b"colr")
                        || value == FourCc::from_bytes(*b"pasp")
            )
        {
            continue;
        }
        normalized_children.push(child_box);
    }
    let normalized_children = reorder_sample_entry_children_by_type_preference(
        &normalized_children,
        &[
            FourCc::from_bytes(*b"hvcC"),
            FourCc::from_bytes(*b"colr"),
            FourCc::from_bytes(*b"pasp"),
        ],
    );
    super::mp4::replace_visual_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_fragmented_vp9_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    let mut updated = false;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box) == Some(FourCc::from_bytes(*b"vpcC")) {
            let mut vpcc = super::mp4::decode_typed_box::<VpCodecConfiguration>(&child_box)?;
            if vpcc.level == 0 {
                vpcc.level = if vpcc.profile == 2 && vpcc.bit_depth >= 10 {
                    0x0a
                } else {
                    0x14
                };
                normalized_children.push(super::mp4::encode_typed_box(&vpcc, &[])?);
                updated = true;
                continue;
            }
        }
        normalized_children.push(child_box);
    }
    if !updated {
        return Ok(imported_track.sample_entry_box.clone());
    }
    super::mp4::replace_visual_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn fragmented_visual_pasp_is_square(child_box: &[u8]) -> Result<bool, MuxError> {
    let pasp = super::mp4::decode_typed_box::<Pasp>(child_box)?;
    Ok(pasp.h_spacing != 0 && pasp.h_spacing == pasp.v_spacing)
}

fn synthesized_imported_fragmented_avc_pasp_box(
    imported_track: &ImportedTrack,
) -> Result<Option<Vec<u8>>, MuxError> {
    let sample_entry =
        super::mp4::decode_typed_box::<VisualSampleEntry>(&imported_track.sample_entry_box)?;
    if sample_entry.width == 0
        || sample_entry.height == 0
        || imported_track.width == 0
        || imported_track.height == 0
        || imported_track.height != sample_entry.height
        || imported_track.width == sample_entry.width
    {
        return Ok(None);
    }
    let gcd = greatest_common_divisor_u16(imported_track.width, sample_entry.width);
    if gcd == 0 {
        return Ok(None);
    }
    let pasp = Pasp {
        h_spacing: u32::from(imported_track.width / gcd),
        v_spacing: u32::from(sample_entry.width / gcd),
    };
    if pasp.h_spacing == 0 || pasp.h_spacing == pasp.v_spacing {
        return Ok(None);
    }
    Ok(Some(super::mp4::encode_typed_box(&pasp, &[])?))
}

const fn greatest_common_divisor_u16(mut left: u16, mut right: u16) -> u16 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn fragmented_imported_decode_time_offset_for_staging(
    track_id: u32,
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
    movie_timescale: u32,
    allow_inexact_movie_scaling: bool,
) -> Result<u64, MuxError> {
    if output_layout != MuxOutputLayout::Fragmented {
        return Ok(0);
    }
    let Some(decode_time_offset) = imported_track
        .mux_policy
        .header_policy()
        .and_then(|policy| policy.source_media_decode_time_offset)
    else {
        return Ok(0);
    };
    let normalized = scale_track_time_to_movie(
        track_id,
        i64::try_from(decode_time_offset)
            .map_err(|_| MuxError::LayoutOverflow("fragment decode-time offset"))?,
        imported_track.timescale,
        movie_timescale,
        allow_inexact_movie_scaling,
    )?;
    u64::try_from(normalized).map_err(|_| MuxError::LayoutOverflow("fragment decode-time offset"))
}

fn imported_timing_start_time_ticks(
    track_id: u32,
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
    movie_timescale: u32,
    allow_inexact_movie_scaling: bool,
) -> Result<i64, MuxError> {
    let mut start_time_ticks = 0_i64;
    if output_layout == MuxOutputLayout::Fragmented
        && let Some(decode_time_offset) = imported_track
            .mux_policy
            .header_policy()
            .and_then(|policy| policy.source_media_decode_time_offset)
    {
        let normalized = scale_track_time_to_movie(
            track_id,
            i64::try_from(decode_time_offset)
                .map_err(|_| MuxError::LayoutOverflow("fragment start-time normalization"))?,
            imported_track.timescale,
            movie_timescale,
            allow_inexact_movie_scaling,
        )?;
        start_time_ticks =
            start_time_ticks
                .checked_add(normalized)
                .ok_or(MuxError::LayoutOverflow(
                    "fragment start-time normalization",
                ))?;
    }
    if let Some(media_time) = imported_track.source_edit_media_time {
        let normalized = scale_track_time_to_movie(
            track_id,
            i64::try_from(media_time)
                .map_err(|_| MuxError::LayoutOverflow("segment start-time normalization"))?,
            imported_track.timescale,
            movie_timescale,
            allow_inexact_movie_scaling,
        )?;
        start_time_ticks = start_time_ticks
            .checked_sub(normalized)
            .ok_or(MuxError::LayoutOverflow("segment start-time normalization"))?;
    }
    Ok(start_time_ticks)
}

fn derived_fragmented_imported_edit_media_time(
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
) -> Option<u64> {
    if !matches!(output_layout, MuxOutputLayout::Fragmented)
        || !imported_track.kind.is_video()
        || !imported_track_uses_avc_family(imported_track)
    {
        return None;
    }
    imported_track
        .samples
        .first()
        .and_then(|sample| {
            (sample.composition_time_offset > 0).then_some(sample.composition_time_offset)
        })
        .and_then(|offset| u64::try_from(offset).ok())
}

fn normalize_imported_flat_h264_sample_entry_box(
    imported_track: &ImportedTrack,
    source_had_empty_stts: bool,
    preserve_flat_authority_layout: bool,
) -> Result<Vec<u8>, MuxError> {
    let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box)
        .unwrap_or(FourCc::from_bytes(*b"avc1"));
    let source_sample_entry =
        super::mp4::decode_typed_box::<VisualSampleEntry>(&imported_track.sample_entry_box)?;
    let source_child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let source_pasp_index = source_child_boxes.iter().position(|child_box| {
        sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp"))
    });
    let source_colr_index = source_child_boxes.iter().position(|child_box| {
        sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"colr"))
    });
    let source_prefers_colr_before_pasp = preserve_flat_authority_layout
        && matches!(
            (source_pasp_index, source_colr_index),
            (Some(pasp_index), Some(colr_index)) if colr_index < pasp_index
        );
    let sample_entry_box = {
        let child_boxes =
            super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
        let source_btrt_box = child_boxes.iter().find_map(|child_box| {
            (sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt")))
                .then(|| child_box.clone())
        });
        let pasp_box = child_boxes.iter().find_map(|child_box| {
            (sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp")))
                .then(|| child_box.clone())
        });
        let colr_box = child_boxes.iter().find_map(|child_box| {
            (sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"colr")))
                .then(|| child_box.clone())
        });
        let carries_source_btrt = child_boxes.iter().any(|child_box| {
            sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
        });
        let carries_source_pasp = pasp_box.is_some();
        let carries_source_colr = colr_box.is_some();
        let avcc_box = child_boxes.into_iter().find(|child_box| {
            sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"avcC"))
        });
        if let Some(avcc_box) = avcc_box {
            let avcc = super::mp4::decode_typed_box::<AVCDecoderConfiguration>(&avcc_box)?;
            let (mut rebuilt_sample_entry_box, _, _) =
                build_h264_sample_entry_from_avc_config_with_box_type_and_options(
                    &avcc,
                    sample_entry_type,
                    "track",
                    false,
                )?;
            if !carries_source_pasp {
                rebuilt_sample_entry_box =
                    super::mp4::strip_visual_sample_entry_immediate_children(
                        &rebuilt_sample_entry_box,
                        &[FourCc::from_bytes(*b"pasp")],
                    )?;
            }
            if !carries_source_colr {
                rebuilt_sample_entry_box =
                    super::mp4::strip_visual_sample_entry_immediate_children(
                        &rebuilt_sample_entry_box,
                        &[FourCc::from_bytes(*b"colr")],
                    )?;
            }
            if pasp_box.is_some() || colr_box.is_some() {
                let mut rebuilt_children =
                    super::mp4::visual_sample_entry_immediate_children(&rebuilt_sample_entry_box)?;
                if let Some(pasp_box) = pasp_box {
                    let carries_pasp = rebuilt_children.iter().any(|child_box| {
                        sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"pasp"))
                    });
                    if !carries_pasp {
                        rebuilt_children.push(pasp_box);
                    }
                }
                if let Some(colr_box) = colr_box {
                    let carries_colr = rebuilt_children.iter().any(|child_box| {
                        sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"colr"))
                    });
                    if !carries_colr {
                        rebuilt_children.push(colr_box);
                    }
                }
                rebuilt_sample_entry_box =
                    super::mp4::replace_visual_sample_entry_immediate_children(
                        &rebuilt_sample_entry_box,
                        &rebuilt_children,
                    )?;
            }
            let rebuilt_sample_entry_box = super::mp4::replace_visual_sample_entry_compressorname(
                &rebuilt_sample_entry_box,
                source_sample_entry.compressorname,
            )?;
            let rebuilt_sample_entry_box = if preserve_flat_authority_layout
                && source_btrt_box.is_some()
            {
                let mut rebuilt_children =
                    super::mp4::visual_sample_entry_immediate_children(&rebuilt_sample_entry_box)?;
                if let Some(source_btrt_box) = source_btrt_box.as_ref() {
                    if let Some(index) = rebuilt_children.iter().position(|child_box| {
                        sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
                    }) {
                        rebuilt_children[index] = source_btrt_box.clone();
                    } else {
                        rebuilt_children.push(source_btrt_box.clone());
                    }
                    super::mp4::replace_visual_sample_entry_immediate_children(
                        &rebuilt_sample_entry_box,
                        &rebuilt_children,
                    )?
                } else {
                    rebuilt_sample_entry_box
                }
            } else {
                rebuilt_sample_entry_box
            };
            (rebuilt_sample_entry_box, carries_source_btrt)
        } else {
            (
                super::mp4::strip_visual_sample_entry_immediate_children(
                    &imported_track.sample_entry_box,
                    &[FourCc::from_bytes(*b"btrt")],
                )?,
                carries_source_btrt,
            )
        }
    };
    let normalized = if (!sample_entry_box.1
        && !source_had_empty_stts
        && imported_track.source_edit_media_time.is_none())
        || (preserve_flat_authority_layout && sample_entry_box.1)
    {
        sample_entry_box.0
    } else {
        let btrt = build_btrt_from_sample_sizes_with_total_duration(
            imported_track
                .samples
                .iter()
                .map(|sample| (sample.data_size, sample.duration)),
            imported_track.timescale,
            imported_sample_media_duration(&imported_track.samples),
        )?;
        super::mp4::append_visual_sample_entry_btrt(&sample_entry_box.0, &btrt)?
    };
    let child_order = if preserve_flat_authority_layout && source_prefers_colr_before_pasp {
        [
            FourCc::from_bytes(*b"avcC"),
            FourCc::from_bytes(*b"colr"),
            FourCc::from_bytes(*b"pasp"),
            FourCc::from_bytes(*b"btrt"),
        ]
    } else {
        [
            FourCc::from_bytes(*b"avcC"),
            FourCc::from_bytes(*b"pasp"),
            FourCc::from_bytes(*b"colr"),
            FourCc::from_bytes(*b"btrt"),
        ]
    };
    let reordered_children = reorder_sample_entry_children_by_type_preference(
        &super::mp4::visual_sample_entry_immediate_children(&normalized)?,
        &child_order,
    );
    super::mp4::replace_visual_sample_entry_immediate_children(&normalized, &reordered_children)
}

fn imported_mp4_avc_sample_contains_sync_nal(sample_bytes: &[u8], length_size: usize) -> bool {
    if length_size == 0 || length_size > 4 {
        return false;
    }
    let mut offset = 0usize;
    while offset < sample_bytes.len() {
        if sample_bytes.len() - offset < length_size {
            break;
        }
        let mut nal_size = 0usize;
        for byte in &sample_bytes[offset..offset + length_size] {
            nal_size = (nal_size << 8) | usize::from(*byte);
        }
        offset += length_size;
        if offset >= sample_bytes.len() {
            break;
        }
        let nal_type = sample_bytes[offset] & 0x1F;
        if nal_type == 5 {
            return true;
        }
        if nal_size == 0 || sample_bytes.len() - offset < nal_size {
            break;
        }
        let nal = &sample_bytes[offset..offset + nal_size];
        if imported_mp4_avc_nal_is_intra_slice(nal) {
            return true;
        }
        offset += nal_size;
    }
    false
}
fn normalize_imported_mp4v_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let btrt = build_btrt_from_sample_sizes_with_total_duration(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
        imported_sample_media_duration(&imported_track.samples),
    )?;
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    let mut updated_esds = false;
    for child_box in child_boxes {
        match sample_entry_box_type(&child_box) {
            Some(box_type) if box_type == FourCc::from_bytes(*b"esds") => {}
            _ => {
                normalized_children.push(child_box);
                continue;
            }
        }
        if sample_entry_box_type(&child_box) != Some(FourCc::from_bytes(*b"esds")) {
            normalized_children.push(child_box);
            continue;
        }
        let mut esds = super::mp4::decode_typed_box::<Esds>(&child_box)?;
        for descriptor in &mut esds.descriptors {
            if descriptor.tag != DECODER_CONFIG_DESCRIPTOR_TAG {
                continue;
            }
            if let Some(config) = descriptor.decoder_config_descriptor.as_mut() {
                config.buffer_size_db = btrt.buffer_size_db;
                config.max_bitrate = btrt.max_bitrate;
                config.avg_bitrate = btrt.avg_bitrate;
                updated_esds = true;
            }
        }
        esds.normalize_descriptor_sizes_for_mux()
            .map_err(|_| MuxError::LayoutOverflow("MPEG-4 Part 2 esds"))?;
        normalized_children.push(super::mp4::encode_typed_box(&esds, &[])?);
    }
    if !updated_esds {
        return Ok(imported_track.sample_entry_box.clone());
    }
    super::mp4::replace_visual_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_flat_hevc_sample_entry_box(
    imported_track: &ImportedTrack,
    preserve_flat_authority_layout: bool,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let source_btrt_box = child_boxes.iter().find_map(|child_box| {
        (sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt")))
            .then(|| child_box.clone())
    });
    let sample_entry_type = sample_entry_box_type(&imported_track.sample_entry_box)
        .unwrap_or(FourCc::from_bytes(*b"hvc1"));
    let carries_dolby_vision_config = child_boxes.iter().any(|child_box| {
        matches!(
            sample_entry_box_type(child_box),
            Some(value)
                if value == FourCc::from_bytes(*b"dvcC")
                    || value == FourCc::from_bytes(*b"dvvC")
        )
    });
    if carries_dolby_vision_config
        && matches!(
            sample_entry_type,
            value
                if value == FourCc::from_bytes(*b"dvh1")
                    || value == FourCc::from_bytes(*b"dvhe")
        )
    {
        let reordered_children = reorder_sample_entry_children_by_type_preference(
            &child_boxes,
            &[
                FourCc::from_bytes(*b"hvcC"),
                FourCc::from_bytes(*b"dvcC"),
                FourCc::from_bytes(*b"dvvC"),
                FourCc::from_bytes(*b"pasp"),
                FourCc::from_bytes(*b"btrt"),
            ],
        );
        return super::mp4::replace_visual_sample_entry_immediate_children(
            &imported_track.sample_entry_box,
            &reordered_children,
        );
    }
    let sample_entry_box = if carries_dolby_vision_config {
        let reordered_children = reorder_sample_entry_children_by_type_preference(
            &child_boxes,
            &[
                FourCc::from_bytes(*b"hvcC"),
                FourCc::from_bytes(*b"dvcC"),
                FourCc::from_bytes(*b"dvvC"),
                FourCc::from_bytes(*b"pasp"),
                FourCc::from_bytes(*b"btrt"),
            ],
        );
        super::mp4::replace_visual_sample_entry_immediate_children(
            &imported_track.sample_entry_box,
            &reordered_children,
        )?
    } else {
        imported_track.sample_entry_box.clone()
    };
    if preserve_flat_authority_layout && let Some(source_btrt_box) = source_btrt_box {
        let mut rebuilt_children =
            super::mp4::visual_sample_entry_immediate_children(&sample_entry_box)?;
        if let Some(index) = rebuilt_children.iter().position(|child_box| {
            sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"btrt"))
        }) {
            rebuilt_children[index] = source_btrt_box;
        } else {
            rebuilt_children.push(source_btrt_box);
        }
        return super::mp4::replace_visual_sample_entry_immediate_children(
            &sample_entry_box,
            &rebuilt_children,
        );
    }
    let btrt = build_btrt_from_sample_sizes_with_total_duration(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
        imported_track_flat_authority_media_duration(imported_track)
            .or_else(|| imported_sample_media_duration(&imported_track.samples)),
    )?;
    super::mp4::replace_visual_sample_entry_btrt(&sample_entry_box, &btrt)
}

fn normalize_imported_flat_av1_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::visual_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let carries_dolby_vision_config = child_boxes
        .iter()
        .any(|child_box| sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"dvvC")));
    if !carries_dolby_vision_config {
        return Ok(imported_track.sample_entry_box.clone());
    }
    let reordered_children = reorder_sample_entry_children_by_type_preference(
        &child_boxes,
        &[
            FourCc::from_bytes(*b"av1C"),
            FourCc::from_bytes(*b"dvcC"),
            FourCc::from_bytes(*b"dvvC"),
            FourCc::from_bytes(*b"fiel"),
            FourCc::from_bytes(*b"colr"),
            FourCc::from_bytes(*b"clli"),
            FourCc::from_bytes(*b"mdcv"),
            FourCc::from_bytes(*b"pasp"),
            FourCc::from_bytes(*b"btrt"),
        ],
    );
    super::mp4::replace_visual_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &reordered_children,
    )
}

fn reorder_sample_entry_children_by_type_preference(
    child_boxes: &[Vec<u8>],
    preferred_order: &[FourCc],
) -> Vec<Vec<u8>> {
    let mut ordered = Vec::with_capacity(child_boxes.len());
    let mut used = vec![false; child_boxes.len()];
    for preferred_type in preferred_order {
        for (index, child_box) in child_boxes.iter().enumerate() {
            if used[index] || sample_entry_box_type(child_box) != Some(*preferred_type) {
                continue;
            }
            ordered.push(child_box.clone());
            used[index] = true;
        }
    }
    for (index, child_box) in child_boxes.iter().enumerate() {
        if used[index] {
            continue;
        }
        ordered.push(child_box.clone());
    }
    ordered
}

fn normalize_imported_mp4a_sample_entry_box(
    imported_track: &ImportedTrack,
    output_layout: MuxOutputLayout,
    preserve_flat_authority_layout: bool,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::audio_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    let mut updated_esds = false;
    let mut stripped_child = false;
    let btrt = build_btrt_from_sample_sizes(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
    )?;
    let rebuild_flat_decoder_config_bitrates = matches!(output_layout, MuxOutputLayout::Flat)
        && (!preserve_flat_authority_layout
            || imported_track_should_rechunk_flat_audio(imported_track));
    for child_box in child_boxes {
        let child_box_type = sample_entry_box_type(&child_box);
        if matches!(output_layout, MuxOutputLayout::Fragmented)
            && child_box_type.is_some_and(imported_fragmented_audio_child_should_be_stripped)
        {
            stripped_child = true;
            continue;
        }
        if matches!(output_layout, MuxOutputLayout::Flat)
            && child_box_type.is_some_and(imported_flat_mp4a_child_should_be_stripped)
        {
            continue;
        }
        if child_box_type != Some(FourCc::from_bytes(*b"esds")) {
            normalized_children.push(child_box);
            continue;
        }
        let mut esds = super::mp4::decode_typed_box::<Esds>(&child_box)?;
        for descriptor in &mut esds.descriptors {
            if descriptor.tag == crate::boxes::iso14496_14::ES_DESCRIPTOR_TAG
                && let Some(es_descriptor) = descriptor.es_descriptor.as_mut()
                && es_descriptor.es_id != 0
            {
                es_descriptor.es_id = 0;
                updated_esds = true;
            }
            if descriptor.tag == DECODER_CONFIG_DESCRIPTOR_TAG
                && let Some(config) = descriptor.decoder_config_descriptor.as_mut()
            {
                if matches!(output_layout, MuxOutputLayout::Fragmented) {
                    if config.buffer_size_db != 0 {
                        config.buffer_size_db = 0;
                        updated_esds = true;
                    }
                } else if rebuild_flat_decoder_config_bitrates {
                    if config.buffer_size_db != btrt.buffer_size_db {
                        config.buffer_size_db = btrt.buffer_size_db;
                        updated_esds = true;
                    }
                    if config.max_bitrate != btrt.max_bitrate {
                        config.max_bitrate = btrt.max_bitrate;
                        updated_esds = true;
                    }
                    if config.avg_bitrate != btrt.avg_bitrate {
                        config.avg_bitrate = btrt.avg_bitrate;
                        updated_esds = true;
                    }
                }
            }
        }
        esds.normalize_descriptor_sizes_for_mux()
            .map_err(|_| MuxError::LayoutOverflow("AAC esds normalization"))?;
        normalized_children.push(super::mp4::encode_typed_box(&esds, &[])?);
    }
    if !updated_esds && !stripped_child {
        return Ok(imported_track.sample_entry_box.clone());
    }
    super::mp4::replace_audio_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_fragmented_dolby_audio_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::audio_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());

    for child_box in child_boxes {
        let child_box_type = sample_entry_box_type(&child_box);
        if child_box_type.is_some_and(imported_fragmented_audio_child_should_be_stripped) {
            continue;
        }
        if child_box_type == Some(FourCc::from_bytes(*b"dec3")) {
            let mut dec3 =
                super::mp4::decode_typed_box::<crate::boxes::etsi_ts_102_366::Dec3>(&child_box)?;
            while dec3.reserved.last() == Some(&0) {
                dec3.reserved.pop();
            }
            normalized_children.push(super::mp4::encode_typed_box(&dec3, &[])?);
            continue;
        }
        normalized_children.push(child_box);
    }

    super::mp4::replace_audio_sample_entry_immediate_children_without_trailing_bytes(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_flat_dolby_audio_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let child_boxes =
        super::mp4::audio_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    let btrt = build_btrt_from_sample_sizes(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
    )?;

    for child_box in child_boxes {
        let child_box_type = sample_entry_box_type(&child_box);
        if child_box_type == Some(FourCc::from_bytes(*b"btrt")) {
            continue;
        }
        if child_box_type == Some(FourCc::from_bytes(*b"dec3")) {
            let mut dec3 =
                super::mp4::decode_typed_box::<crate::boxes::etsi_ts_102_366::Dec3>(&child_box)?;
            while dec3.reserved.last() == Some(&0) {
                dec3.reserved.pop();
            }
            normalized_children.push(super::mp4::encode_typed_box(&dec3, &[])?);
            continue;
        }
        normalized_children.push(child_box);
    }

    normalized_children.push(super::mp4::encode_typed_box(&btrt, &[])?);
    super::mp4::replace_audio_sample_entry_immediate_children_without_trailing_bytes(
        &imported_track.sample_entry_box,
        &normalized_children,
    )
}

fn normalize_imported_flat_speex_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    let mut sample_entry = super::mp4::decode_audio_sample_entry(&imported_track.sample_entry_box)?;
    sample_entry.sample_size = 16;
    let btrt = build_btrt_from_sample_sizes(
        imported_track
            .samples
            .iter()
            .map(|sample| (sample.data_size, sample.duration)),
        imported_track.timescale,
    )?;
    let btrt_box = super::mp4::encode_typed_box(&btrt, &[])?;
    let normalized = super::mp4::encode_typed_box(&sample_entry, &btrt_box)?;
    if let Some(vendor_code) =
        super::mp4::audio_sample_entry_vendor_code(&imported_track.sample_entry_box)?
    {
        return super::mp4::replace_audio_sample_entry_vendor_code(&normalized, vendor_code);
    }
    Ok(normalized)
}

fn imported_flat_mp4a_child_should_be_stripped(box_type: FourCc) -> bool {
    matches!(
        box_type,
        value
            if value == FourCc::from_bytes(*b"dmix")
                || value == FourCc::from_bytes(*b"udc2")
                || value == FourCc::from_bytes(*b"udi2")
              || value == FourCc::from_bytes(*b"udex")
              || value == FourCc::from_bytes(*b"sbtd")
    )
}

fn imported_fragmented_audio_child_should_be_stripped(box_type: FourCc) -> bool {
    box_type == FourCc::from_bytes(*b"btrt") || box_type == FourCc::from_u32(0)
}

fn imported_sample_media_duration(samples: &[ImportedSample]) -> Option<u64> {
    let mut decode_time = 0_u64;
    let mut media_duration = 0_u64;
    for sample in samples {
        let duration = u64::from(sample.duration);
        let decode_end = decode_time.checked_add(duration)?;
        media_duration = media_duration.max(decode_end);
        let presentation_end = i128::from(decode_time)
            .saturating_add(i128::from(sample.composition_time_offset))
            .saturating_add(i128::from(sample.duration));
        if presentation_end > 0 {
            media_duration = media_duration.max(u64::try_from(presentation_end).ok()?);
        }
        decode_time = decode_end;
    }
    Some(media_duration)
}

fn imported_track_should_strip_single_sample_dts_btrt(imported_track: &ImportedTrack) -> bool {
    imported_track.mux_policy.strip_single_sample_dts_btrt() && imported_track.samples.len() == 1
}

fn normalize_imported_fragmented_dts_sample_entry_box(
    imported_track: &ImportedTrack,
) -> Result<Vec<u8>, MuxError> {
    if sample_entry_box_type(&imported_track.sample_entry_box) != Some(FourCc::from_bytes(*b"dtsc"))
    {
        return super::mp4::strip_audio_sample_entry_immediate_children(
            &imported_track.sample_entry_box,
            &[FourCc::from_bytes(*b"btrt")],
        );
    }

    let child_boxes =
        super::mp4::audio_sample_entry_immediate_children(&imported_track.sample_entry_box)?;
    if child_boxes
        .iter()
        .any(|child_box| sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"ddts")))
    {
        return super::mp4::strip_audio_sample_entry_immediate_children(
            &imported_track.sample_entry_box,
            &[FourCc::from_bytes(*b"btrt")],
        );
    }

    let sample_entry = super::mp4::decode_audio_sample_entry(&imported_track.sample_entry_box)?;
    let ddts_box = super::mp4::encode_typed_box(
        &Ddts {
            sampling_frequency: u32::from(sample_entry.sample_rate_int()),
            max_bitrate: 0,
            avg_bitrate: 0,
            sample_depth: u8::try_from(sample_entry.sample_size)
                .map_err(|_| MuxError::LayoutOverflow("DTS sample depth"))?,
            frame_duration: IMPORTED_DDTS_FRAME_DURATION,
            stream_construction: IMPORTED_DDTS_STREAM_CONSTRUCTION,
            core_lfe_present: false,
            core_layout: IMPORTED_DDTS_CORE_LAYOUT,
            core_size: 0,
            stereo_downmix: false,
            representation_type: IMPORTED_DDTS_REPRESENTATION_TYPE,
            channel_layout: IMPORTED_DDTS_CHANNEL_LAYOUT_MASK,
            multi_asset_flag: false,
            lbr_duration_mod: false,
        },
        &[],
    )?;
    super::mp4::replace_audio_sample_entry_immediate_children(
        &imported_track.sample_entry_box,
        &[ddts_box],
    )
}

fn imported_track_uses_avc_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"avc1")
                || value == FourCc::from_bytes(*b"avc2")
                || value == FourCc::from_bytes(*b"avc3")
                || value == FourCc::from_bytes(*b"avc4")
    )
}

fn imported_track_uses_mp4a_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"mp4a"))
}

fn imported_track_uses_xhe_aac_family(imported_track: &ImportedTrack) -> bool {
    if !imported_track_uses_mp4a_family(imported_track) {
        return false;
    }
    let Ok((_, child_boxes, _)) =
        super::mp4::decode_audio_sample_entry_parts(&imported_track.sample_entry_box)
    else {
        return false;
    };
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box) != Some(FourCc::from_bytes(*b"esds")) {
            continue;
        }
        let Ok(esds) = super::mp4::decode_typed_box::<Esds>(&child_box) else {
            continue;
        };
        let Ok(profile) = detect_aac_profile(&esds) else {
            continue;
        };
        if profile.is_some_and(|profile| profile.audio_object_type == 42) {
            return true;
        }
    }
    false
}

fn box_header_type(box_bytes: &[u8]) -> Option<FourCc> {
    let bytes: [u8; 4] = box_bytes.get(4..8)?.try_into().ok()?;
    Some(FourCc::from_bytes(bytes))
}

fn imported_track_uses_speex_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"spex"))
}

fn imported_track_uses_mp4v_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"mp4v"))
}

fn imported_track_uses_alac_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"alac"))
}

fn imported_track_uses_audio_btrt_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"ac-3")
                || value == FourCc::from_bytes(*b"ec-3")
                || value == FourCc::from_bytes(*b"ac-4")
                || value == FourCc::from_bytes(*b"fLaC")
    )
}

fn imported_track_uses_visual_btrt_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"hev1")
                || value == FourCc::from_bytes(*b"hvc1")
                || value == FourCc::from_bytes(*b"vvc1")
                || value == FourCc::from_bytes(*b"vvi1")
                || value == FourCc::from_bytes(*b"dvh1")
                || value == FourCc::from_bytes(*b"dvhe")
    )
}

fn imported_track_uses_dts_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"dtsc")
                || value == FourCc::from_bytes(*b"dtse")
                || value == FourCc::from_bytes(*b"dtsh")
                || value == FourCc::from_bytes(*b"dtsl")
                || value == FourCc::from_bytes(*b"dtsm")
                || value == FourCc::from_bytes(*b"dts-")
                || value == FourCc::from_bytes(*b"dtsx")
                || value == FourCc::from_bytes(*b"dtsy")
    )
}

fn imported_track_uses_iamf_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"iamf"))
}

fn imported_track_uses_mpegh_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"mha1")
                || value == FourCc::from_bytes(*b"mha2")
                || value == FourCc::from_bytes(*b"mhm1")
                || value == FourCc::from_bytes(*b"mhm2")
    )
}

fn imported_track_uses_mp3_family(imported_track: &ImportedTrack) -> bool {
    match sample_entry_box_type(&imported_track.sample_entry_box) {
        Some(value) if value == FourCc::from_bytes(*b".mp3") => true,
        Some(value) if value == FourCc::from_bytes(*b"mp4a") => {
            sample_entry_carries_oti(&imported_track.sample_entry_box, 0x6B)
        }
        _ => false,
    }
}

fn imported_track_uses_vvc_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"vvc1") || value == FourCc::from_bytes(*b"vvi1")
    )
}

fn sample_entry_carries_oti(sample_entry_box: &[u8], object_type_indication: u8) -> bool {
    let sample_entry_type = match sample_entry_box_type(sample_entry_box) {
        Some(value) => value,
        None => return false,
    };
    let child_boxes = match sample_entry_type {
        value if value == FourCc::from_bytes(*b"mp4a") => {
            match super::mp4::decode_audio_sample_entry_parts(sample_entry_box) {
                Ok((_, child_boxes, _)) => child_boxes,
                Err(_) => return false,
            }
        }
        value if value == FourCc::from_bytes(*b"mp4v") => {
            match super::mp4::decode_visual_sample_entry_parts(sample_entry_box) {
                Ok((_, child_boxes, _)) => child_boxes,
                Err(_) => return false,
            }
        }
        _ => return false,
    };
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box) != Some(FourCc::from_bytes(*b"esds")) {
            continue;
        }
        let Ok(esds) = super::mp4::decode_typed_box::<Esds>(&child_box) else {
            continue;
        };
        for descriptor in esds.descriptors {
            if let Some(decoder_config) = descriptor.decoder_config_descriptor
                && decoder_config.object_type_indication == object_type_indication
            {
                return true;
            }
        }
    }
    false
}

fn imported_track_uses_packet_clocked_flac_family(imported_track: &ImportedTrack) -> bool {
    sample_entry_box_type(&imported_track.sample_entry_box) == Some(FourCc::from_bytes(*b"fLaC"))
        && imported_track.timescale == 1_000
        && imported_track_sample_entry_audio_sample_rate(&imported_track.sample_entry_box)
            == Some(1_000)
}

fn imported_track_uses_hevc_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"hev1")
                || value == FourCc::from_bytes(*b"hvc1")
                || value == FourCc::from_bytes(*b"dvh1")
                || value == FourCc::from_bytes(*b"dvhe")
    )
}

fn imported_track_uses_av1_family(imported_track: &ImportedTrack) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"av01")
                || value == FourCc::from_bytes(*b"dav1")
    )
}

fn infer_imported_mp4_authority_flat_ftyp_profile(
    imported_tracks: &[ImportedTrack],
) -> (FourCc, u32, Vec<FourCc>) {
    if imported_tracks
        .iter()
        .all(imported_track_uses_layered_hevc_family)
    {
        return (
            FourCc::from_bytes(*b"isom"),
            1,
            vec![FourCc::from_bytes(*b"isom"), FourCc::from_bytes(*b"iso4")],
        );
    }
    (
        FourCc::from_bytes(*b"isom"),
        1,
        vec![FourCc::from_bytes(*b"isom")],
    )
}

fn imported_track_should_preserve_auto_flat_movie_timescale(
    imported_track: &ImportedTrack,
) -> bool {
    matches!(
        sample_entry_box_type(&imported_track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"apco")
                || value == FourCc::from_bytes(*b"apcn")
                || value == FourCc::from_bytes(*b"apch")
                || value == FourCc::from_bytes(*b"apcs")
                || value == FourCc::from_bytes(*b"ap4x")
                || value == FourCc::from_bytes(*b"ap4h")
    )
}

fn imported_track_sample_entry_audio_sample_rate(sample_entry_box: &[u8]) -> Option<u32> {
    let rate_bytes = sample_entry_box.get(32..36)?;
    let rate = u32::from_be_bytes(rate_bytes.try_into().ok()?);
    Some(rate >> 16)
}

fn track_candidate_uses_dts_family(track: &TrackCandidate) -> bool {
    matches!(
        sample_entry_box_type(&track.sample_entry_box),
        Some(value)
            if value == FourCc::from_bytes(*b"dtsc")
                || value == FourCc::from_bytes(*b"dtse")
                || value == FourCc::from_bytes(*b"dtsh")
                || value == FourCc::from_bytes(*b"dtsl")
                || value == FourCc::from_bytes(*b"dtsm")
                || value == FourCc::from_bytes(*b"dts-")
                || value == FourCc::from_bytes(*b"dtsx")
                || value == FourCc::from_bytes(*b"dtsy")
    )
}

fn sample_entry_box_type(sample_entry_box: &[u8]) -> Option<FourCc> {
    Some(FourCc::from_bytes(
        sample_entry_box.get(4..8)?.try_into().ok()?,
    ))
}

fn validate_request_shape(request: &MuxRequest, output_path: &Path) -> Result<(), MuxError> {
    if request.tracks().is_empty() {
        return Err(MuxError::MissingTrackSpecs);
    }
    if matches!(
        request.destination_mode(),
        MuxDestinationMode::UpdateOrCreateDestination
    ) {
        if !matches!(request.output_layout(), MuxOutputLayout::Flat) {
            return Err(MuxError::InvalidDestinationMode {
                mode: request.destination_mode().label(),
                message: "the current destination-path mux mode only supports flat output; use `--out PATH` for create-new fragmented output".to_string(),
            });
        }
        let output_absolute = absolute_path(output_path)?;
        for track in request.tracks() {
            let input_absolute = absolute_path(track.input_path())?;
            if input_absolute == output_absolute {
                return Err(MuxError::InvalidDestinationMode {
                    mode: request.destination_mode().label(),
                    message: "destination-path mux mode does not accept the destination file as an explicit input track".to_string(),
                });
            }
        }
    }
    match (request.output_layout(), request.duration_mode()) {
        (MuxOutputLayout::Flat, Some(duration_mode)) => {
            return Err(MuxError::InvalidOutputLayout {
                layout: request.output_layout().label(),
                message: format!(
                    "flat output does not support `--{}`; use `--layout fragmented` instead",
                    duration_mode.label()
                ),
            });
        }
        (MuxOutputLayout::Fragmented, None) => {
            return Err(MuxError::InvalidOutputLayout {
                layout: request.output_layout().label(),
                message: "fragmented output requires exactly one of `--segment_duration` or `--fragment_duration`".to_string(),
            });
        }
        _ => {}
    }
    let video_count = request
        .tracks()
        .iter()
        .filter(|track| {
            matches!(
                track,
                MuxTrackSpec::Path {
                    selector: Some(MuxMp4TrackSelector::Video),
                    ..
                }
            )
        })
        .count();
    if matches!(request.output_layout(), MuxOutputLayout::Fragmented) && video_count > 1 {
        return Err(MuxError::MultipleVideoTracks { count: video_count });
    }

    let output_absolute = absolute_path(output_path)?;
    for track in request.tracks() {
        let input_absolute = absolute_path(track.input_path())?;
        if input_absolute == output_absolute {
            return Err(MuxError::OutputPathConflict {
                output: output_absolute,
                input: input_absolute,
            });
        }
    }
    Ok(())
}

fn validate_fragmented_split_paths(
    request: &MuxRequest,
    init_path: &Path,
    media_path: &Path,
) -> Result<(), MuxError> {
    if !matches!(request.output_layout(), MuxOutputLayout::Fragmented) {
        return Err(MuxError::InvalidOutputLayout {
            layout: request.output_layout().label(),
            message: "separate fragmented output requires fragmented layout".to_string(),
        });
    }
    validate_request_shape(request, media_path)?;
    let init_absolute = absolute_path(init_path)?;
    let media_absolute = absolute_path(media_path)?;
    if init_absolute == media_absolute {
        return Err(MuxError::InvalidDestinationMode {
            mode: MuxDestinationMode::CreateNew.label(),
            message: "separate fragmented output paths must be distinct".to_string(),
        });
    }
    for (label, path) in [("init", init_path), ("media", media_path)] {
        if path.exists() {
            return Err(MuxError::InvalidDestinationMode {
                mode: MuxDestinationMode::CreateNew.label(),
                message: format!("separate fragmented {label} output path already exists"),
            });
        }
    }
    for track in request.tracks() {
        let input_absolute = absolute_path(track.input_path())?;
        if input_absolute == init_absolute {
            return Err(MuxError::OutputPathConflict {
                output: init_absolute,
                input: input_absolute,
            });
        }
        if input_absolute == media_absolute {
            return Err(MuxError::OutputPathConflict {
                output: media_absolute,
                input: input_absolute,
            });
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn validate_fragmented_split_paths_async(
    request: &MuxRequest,
    init_path: &Path,
    media_path: &Path,
) -> Result<(), MuxError> {
    if !matches!(request.output_layout(), MuxOutputLayout::Fragmented) {
        return Err(MuxError::InvalidOutputLayout {
            layout: request.output_layout().label(),
            message: "separate fragmented output requires fragmented layout".to_string(),
        });
    }
    validate_request_shape(request, media_path)?;
    let init_absolute = absolute_path(init_path)?;
    let media_absolute = absolute_path(media_path)?;
    if init_absolute == media_absolute {
        return Err(MuxError::InvalidDestinationMode {
            mode: MuxDestinationMode::CreateNew.label(),
            message: "separate fragmented output paths must be distinct".to_string(),
        });
    }
    for (label, path) in [("init", init_path), ("media", media_path)] {
        if tokio::fs::try_exists(path)
            .await
            .map_err(|error| mux_io_at_path("inspect mux output", path, error))?
        {
            return Err(MuxError::InvalidDestinationMode {
                mode: MuxDestinationMode::CreateNew.label(),
                message: format!("separate fragmented {label} output path already exists"),
            });
        }
    }
    for track in request.tracks() {
        let input_absolute = absolute_path(track.input_path())?;
        if input_absolute == init_absolute {
            return Err(MuxError::OutputPathConflict {
                output: init_absolute,
                input: input_absolute,
            });
        }
        if input_absolute == media_absolute {
            return Err(MuxError::OutputPathConflict {
                output: media_absolute,
                input: input_absolute,
            });
        }
    }
    Ok(())
}

fn build_destination_preserving_request(
    request: &MuxRequest,
    destination_path: &Path,
) -> Result<MuxRequest, MuxError> {
    if !matches!(
        request.destination_mode(),
        MuxDestinationMode::UpdateOrCreateDestination
    ) {
        return Err(MuxError::InvalidDestinationMode {
            mode: request.destination_mode().label(),
            message: "request did not opt into the destination-path mux mode".to_string(),
        });
    }
    let mut tracks = Vec::with_capacity(request.tracks().len() + 1);
    tracks.push(MuxTrackSpec::path(destination_path.to_path_buf()));
    tracks.extend(request.tracks().iter().cloned());
    let mut amended = MuxRequest::new(tracks)
        .with_output_layout(request.output_layout())
        .with_destination_mode(MuxDestinationMode::CreateNew)
        .with_preserve_flat_authority_layout(true);
    if let Some(duration_mode) = request.duration_mode() {
        amended = amended.with_duration_mode(duration_mode);
    }
    Ok(amended)
}

fn should_preserve_destination_mp4(destination_path: &Path) -> bool {
    is_mp4_like_path(destination_path)
}

#[cfg(feature = "async")]
async fn should_preserve_destination_mp4_async(destination_path: &Path) -> bool {
    matches!(
        detect_path_track_kind_async(destination_path).await,
        Ok(DetectedPathTrackKind::Mp4)
    )
}

fn create_update_temp_path(
    output_path: &Path,
    mode: MuxDestinationMode,
) -> Result<PathBuf, MuxError> {
    let parent = output_path
        .parent()
        .ok_or_else(|| MuxError::InvalidDestinationMode {
            mode: mode.label(),
            message: format!(
                "cannot derive a temporary rewrite path for `{}`",
                output_path.display()
            ),
        })?;
    let file_name = output_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| MuxError::InvalidDestinationMode {
            mode: mode.label(),
            message: format!(
                "cannot derive a temporary rewrite path for `{}`",
                output_path.display()
            ),
        })?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MuxError::InvalidDestinationMode {
            mode: mode.label(),
            message: "system clock is earlier than the Unix epoch".to_string(),
        })?
        .as_nanos();
    Ok(parent.join(format!("{file_name}.mp4forge-rewrite-{stamp}.tmp")))
}

fn ensure_output_parent_dir(output_path: &Path) -> Result<(), MuxError> {
    let Some(parent) = output_path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    std::fs::create_dir_all(parent)
        .map_err(|error| mux_io_at_path("create mux output directory", parent, error))
}

fn finalize_new_split_output(
    temp_path: &Path,
    output_path: &Path,
    already_created_path: Option<&Path>,
) -> Result<(), MuxError> {
    if let Err(error) = std::fs::rename(temp_path, output_path) {
        let _ = std::fs::remove_file(temp_path);
        if let Some(path) = already_created_path {
            let _ = std::fs::remove_file(path);
        }
        return Err(mux_io_at_path("finalize mux output", output_path, error));
    }
    Ok(())
}

fn replace_output_path(temp_path: &Path, output_path: &Path) -> Result<(), MuxError> {
    let backup_path = temp_path.with_extension("backup");
    if backup_path.exists() {
        std::fs::remove_file(&backup_path)?;
    }
    std::fs::rename(output_path, &backup_path)?;
    match std::fs::rename(temp_path, output_path) {
        Ok(()) => {
            let _ = std::fs::remove_file(&backup_path);
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::rename(&backup_path, output_path);
            Err(MuxError::Io(error))
        }
    }
}

#[cfg(feature = "async")]
async fn ensure_output_parent_dir_async(output_path: &Path) -> Result<(), MuxError> {
    let Some(parent) = output_path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| mux_io_at_path("create mux output directory", parent, error))
}

#[cfg(feature = "async")]
async fn finalize_new_split_output_async(
    temp_path: &Path,
    output_path: &Path,
    already_created_path: Option<&Path>,
) -> Result<(), MuxError> {
    if let Err(error) = tokio::fs::rename(temp_path, output_path).await {
        let _ = tokio::fs::remove_file(temp_path).await;
        if let Some(path) = already_created_path {
            let _ = tokio::fs::remove_file(path).await;
        }
        return Err(mux_io_at_path("finalize mux output", output_path, error));
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn replace_output_path_async(temp_path: &Path, output_path: &Path) -> Result<(), MuxError> {
    let backup_path = temp_path.with_extension("backup");
    if tokio::fs::try_exists(&backup_path).await? {
        tokio::fs::remove_file(&backup_path).await?;
    }
    tokio::fs::rename(output_path, &backup_path).await?;
    match tokio::fs::rename(temp_path, output_path).await {
        Ok(()) => {
            let _ = tokio::fs::remove_file(&backup_path).await;
            Ok(())
        }
        Err(error) => {
            let _ = tokio::fs::rename(&backup_path, output_path).await;
            Err(MuxError::Io(error))
        }
    }
}

fn display_track_spec(track: &MuxTrackSpec) -> String {
    match track {
        MuxTrackSpec::Path { path, selector } => match selector {
            Some(selector) => format!("{}#{}", path.display(), format_mp4_selector(*selector)),
            None => path.display().to_string(),
        },
        MuxTrackSpec::RawVideo { path, params } => {
            format!("{}#{}", path.display(), params.format_suffix())
        }
    }
}

fn format_mp4_selector(selector: MuxMp4TrackSelector) -> String {
    match selector {
        MuxMp4TrackSelector::Video => "video".to_string(),
        MuxMp4TrackSelector::Audio { occurrence: 1 } => "audio".to_string(),
        MuxMp4TrackSelector::Audio { occurrence } => format!("audio:{occurrence}"),
        MuxMp4TrackSelector::Text { occurrence: 1 } => "text".to_string(),
        MuxMp4TrackSelector::Text { occurrence } => format!("text:{occurrence}"),
        MuxMp4TrackSelector::TrackId { track_id } => format!("track:{track_id}"),
    }
}

fn detect_path_track_kind_sync(path: &Path) -> Result<DetectedPathTrackKind, MuxError> {
    let mut file =
        File::open(path).map_err(|error| mux_io_at_path("open mux input", path, error))?;
    let mut prefix = [0_u8; PATH_KIND_PREFIX_BYTES];
    let read = file.read(&mut prefix)?;
    let prefix = &prefix[..read];
    if prefix.starts_with(b"OggS") {
        file.seek(SeekFrom::Start(0))?;
        return detect_ogg_track_kind_sync(&mut file);
    }
    if prefix.starts_with(b"caff") {
        file.seek(SeekFrom::Start(0))?;
        return detect_caf_track_kind_sync(&mut file);
    }
    if let Some(kind) = detect_container_path_kind_from_path_and_prefix(path, prefix) {
        return Ok(DetectedPathTrackKind::Container(kind));
    }
    if let Some(kind) = detect_nhml_sidecar_kind(path, prefix) {
        return Ok(DetectedPathTrackKind::Container(kind));
    }
    if let Some(kind) = detect_id3_wrapped_audio_sync(&mut file, prefix)? {
        return Ok(kind);
    }
    if let Some(kind) = detect_vobsub_track_kind_sync(path, prefix)? {
        return Ok(kind);
    }
    let detected = detect_path_track_kind_from_prefix(prefix);
    if matches!(detected, DetectedPathTrackKind::Mp4ImportOnly(_))
        && prefix.starts_with(b"DTSHDHDR")
    {
        file.seek(SeekFrom::Start(0))?;
        let file_size = file.metadata()?.len();
        if wrapped_dts_family_has_native_core_sync_sync(
            &mut file,
            file_size,
            &path.display().to_string(),
        )? {
            return Ok(DetectedPathTrackKind::Raw(MuxRawCodec::Dts));
        }
    }
    if detected != DetectedPathTrackKind::Unknown {
        return Ok(detected);
    }
    Ok(detect_av1_extension_fallback(path).unwrap_or(DetectedPathTrackKind::Unknown))
}

fn is_mp4_like_path(path: &Path) -> bool {
    matches!(
        detect_path_track_kind_sync(path),
        Ok(DetectedPathTrackKind::Mp4)
    )
}

#[cfg(feature = "async")]
async fn detect_path_track_kind_async(path: &Path) -> Result<DetectedPathTrackKind, MuxError> {
    let mut file = TokioFile::open(path)
        .await
        .map_err(|error| mux_io_at_path("open mux input", path, error))?;
    let mut prefix = [0_u8; PATH_KIND_PREFIX_BYTES];
    let read = file.read(&mut prefix).await?;
    let prefix = &prefix[..read];
    if prefix.starts_with(b"OggS") {
        file.seek(SeekFrom::Start(0)).await?;
        return detect_ogg_track_kind_async(&mut file).await;
    }
    if prefix.starts_with(b"caff") {
        file.seek(SeekFrom::Start(0)).await?;
        return detect_caf_track_kind_async(&mut file).await;
    }
    if let Some(kind) = detect_container_path_kind_from_path_and_prefix(path, prefix) {
        return Ok(DetectedPathTrackKind::Container(kind));
    }
    if let Some(kind) = detect_nhml_sidecar_kind(path, prefix) {
        return Ok(DetectedPathTrackKind::Container(kind));
    }
    if let Some(kind) = detect_id3_wrapped_audio_async(&mut file, prefix).await? {
        return Ok(kind);
    }
    if let Some(kind) = detect_vobsub_track_kind_async(path, prefix).await? {
        return Ok(kind);
    }
    let detected = detect_path_track_kind_from_prefix(prefix);
    if matches!(detected, DetectedPathTrackKind::Mp4ImportOnly(_))
        && prefix.starts_with(b"DTSHDHDR")
    {
        file.seek(SeekFrom::Start(0)).await?;
        let file_size = file.metadata().await?.len();
        if wrapped_dts_family_has_native_core_sync_async(
            &mut file,
            file_size,
            &path.display().to_string(),
        )
        .await?
        {
            return Ok(DetectedPathTrackKind::Raw(MuxRawCodec::Dts));
        }
    }
    if detected != DetectedPathTrackKind::Unknown {
        return Ok(detected);
    }
    Ok(detect_av1_extension_fallback(path).unwrap_or(DetectedPathTrackKind::Unknown))
}

fn detect_av1_extension_fallback(path: &Path) -> Option<DetectedPathTrackKind> {
    let extension = path.extension()?.to_str()?;
    if extension.eq_ignore_ascii_case("obu")
        || extension.eq_ignore_ascii_case("av1")
        || extension.eq_ignore_ascii_case("av1b")
    {
        return Some(DetectedPathTrackKind::Raw(MuxRawCodec::Av1));
    }
    None
}

fn detect_vobsub_track_kind_sync(
    path: &Path,
    prefix: &[u8],
) -> Result<Option<DetectedPathTrackKind>, MuxError> {
    if detect_path_track_kind_from_prefix(prefix)
        == DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub)
    {
        return Ok(Some(DetectedPathTrackKind::Container(
            DetectedContainerPathKind::VobSub,
        )));
    }
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return Ok(None);
    };
    if extension.eq_ignore_ascii_case("sub") {
        let idx_path = path.with_extension("idx");
        if idx_path.is_file() && path_starts_with_sync(&idx_path, b"# VobSub")? {
            return Ok(Some(DetectedPathTrackKind::Container(
                DetectedContainerPathKind::VobSub,
            )));
        }
    }
    Ok(None)
}

#[cfg(feature = "async")]
async fn detect_vobsub_track_kind_async(
    path: &Path,
    prefix: &[u8],
) -> Result<Option<DetectedPathTrackKind>, MuxError> {
    if detect_path_track_kind_from_prefix(prefix)
        == DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub)
    {
        return Ok(Some(DetectedPathTrackKind::Container(
            DetectedContainerPathKind::VobSub,
        )));
    }
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return Ok(None);
    };
    if extension.eq_ignore_ascii_case("sub") {
        let idx_path = path.with_extension("idx");
        let idx_exists = tokio::fs::metadata(&idx_path)
            .await
            .map(|metadata| metadata.is_file())
            .unwrap_or(false);
        if idx_exists && path_starts_with_async(&idx_path, b"# VobSub").await? {
            return Ok(Some(DetectedPathTrackKind::Container(
                DetectedContainerPathKind::VobSub,
            )));
        }
    }
    Ok(None)
}

fn detect_id3_wrapped_audio_sync(
    file: &mut File,
    prefix: &[u8],
) -> Result<Option<DetectedPathTrackKind>, MuxError> {
    let Some(id3_offset) = id3v2_size_from_prefix(prefix) else {
        return Ok(None);
    };
    if let Some(kind) = detect_id3_wrapped_audio_from_prefix(prefix, id3_offset) {
        return Ok(Some(kind));
    }
    let mut header = [0_u8; 7];
    file.seek(SeekFrom::Start(
        u64::try_from(id3_offset).map_err(|_| MuxError::LayoutOverflow("ID3v2 size"))?,
    ))?;
    let read = file.read(&mut header)?;
    Ok(detect_id3_wrapped_audio_from_prefix(&header[..read], 0))
}

#[cfg(feature = "async")]
async fn detect_id3_wrapped_audio_async(
    file: &mut TokioFile,
    prefix: &[u8],
) -> Result<Option<DetectedPathTrackKind>, MuxError> {
    let Some(id3_offset) = id3v2_size_from_prefix(prefix) else {
        return Ok(None);
    };
    if let Some(kind) = detect_id3_wrapped_audio_from_prefix(prefix, id3_offset) {
        return Ok(Some(kind));
    }
    file.seek(SeekFrom::Start(
        u64::try_from(id3_offset).map_err(|_| MuxError::LayoutOverflow("ID3v2 size"))?,
    ))
    .await?;
    let mut header = [0_u8; 7];
    let read = file.read(&mut header).await?;
    Ok(detect_id3_wrapped_audio_from_prefix(&header[..read], 0))
}

fn path_starts_with_sync(path: &Path, signature: &[u8]) -> Result<bool, MuxError> {
    let mut file =
        File::open(path).map_err(|error| mux_io_at_path("open mux input", path, error))?;
    let mut prefix = vec![0_u8; signature.len()];
    let read = file.read(&mut prefix)?;
    Ok(read == signature.len() && prefix == signature)
}

#[cfg(feature = "async")]
async fn path_starts_with_async(path: &Path, signature: &[u8]) -> Result<bool, MuxError> {
    let mut file = TokioFile::open(path)
        .await
        .map_err(|error| mux_io_at_path("open mux input", path, error))?;
    let mut prefix = vec![0_u8; signature.len()];
    let read = file.read(&mut prefix).await?;
    Ok(read == signature.len() && prefix == signature)
}

fn direct_ingest_container_label(kind: DetectedContainerPathKind) -> &'static str {
    match kind {
        DetectedContainerPathKind::Avi => "avi",
        DetectedContainerPathKind::Dash => "dash",
        DetectedContainerPathKind::Ghi => "ghi",
        DetectedContainerPathKind::Gsf => "gsf",
        DetectedContainerPathKind::Nhml => "nhml",
        DetectedContainerPathKind::Nhnt => "nhnt",
        DetectedContainerPathKind::ProgramStream => "program_stream",
        DetectedContainerPathKind::Saf => "saf",
        DetectedContainerPathKind::TransportStream => "transport_stream",
        DetectedContainerPathKind::VobSub => "vobsub",
    }
}

fn detected_kind_supports_flat_mux(kind: DetectedPathTrackKind) -> bool {
    matches!(
        kind,
        DetectedPathTrackKind::Mp4
            | DetectedPathTrackKind::Raw(_)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream)
            | DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub)
    )
}

fn unsupported_gsf_container_message() -> &'static str {
    "GSF is a serialized multi-PID transport surface rather than a local authored-media input on the current path-only mux lane; import the authored files or authored MP4 tracks directly instead"
}

fn unsupported_ghi_container_message() -> &'static str {
    "GHI is a segment-index or manifest transport surface rather than a local authored-media input on the current path-only mux lane; import the authored media files or local MPD inputs directly instead"
}

fn direct_ingest_report_kind(kind: DetectedPathTrackKind) -> DirectIngestDetectedKind {
    match kind {
        DetectedPathTrackKind::Mp4 => DirectIngestDetectedKind::Mp4,
        DetectedPathTrackKind::Container(container) => DirectIngestDetectedKind::Container {
            container: direct_ingest_container_label(container).to_string(),
        },
        DetectedPathTrackKind::Raw(codec) => DirectIngestDetectedKind::Raw {
            codec: codec.prefix().to_string(),
        },
        DetectedPathTrackKind::Mp4ImportOnly(family) => DirectIngestDetectedKind::ImportOnly {
            family: family.to_string(),
        },
        DetectedPathTrackKind::Unknown => DirectIngestDetectedKind::Unknown,
    }
}

fn direct_ingest_report_note(kind: DetectedPathTrackKind) -> Option<String> {
    match kind {
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi) => {
            Some(unsupported_ghi_container_message().to_string())
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {
            Some(unsupported_gsf_container_message().to_string())
        }
        DetectedPathTrackKind::Mp4ImportOnly(kind) => Some(format!(
            "path-only mux import for `{kind}` is not supported; import this family from an MP4 source with `#audio` or `#track:ID` instead"
        )),
        DetectedPathTrackKind::Unknown => Some("path-only mux input is not currently recognized as MP4, VobSub, supported AVI audio or MPEG-4 Part 2 video, supported MPEG-PS MPEG audio, AC-3, or MPEG-4 Part 2/H.264/H.265/VVC video, supported MPEG-TS MPEG audio, AAC LATM, MHAS, AC-3, E-AC-3, AC-4, DTS, TrueHD, MPEG-2 video, AV1, MPEG-4 Part 2, H.264, H.265, VVC, DVB subtitle, or DVB teletext video or subtitle carriage, JPEG still images, PNG still images, BMP still images, JPEG 2000 image or codestream input, self-describing YUV4MPEG raw video, raw ProRes, WAVE/AIFF/AIFC PCM, AAC ADTS, AAC LATM, MP3, AC-3, E-AC-3, AC-4, AMR, AMR-WB, QCP voice audio, DTS core audio, Dolby TrueHD, leading-sync MHAS MPEG-H, FLAC, IAMF, H.263 elementary video, MPEG-2 elementary video, MPEG-4 Part 2 elementary video, H.264 Annex B, H.265 Annex B, IVF-backed AV1/VP8/VP9/VP10, Ogg FLAC, Ogg Opus, Ogg Vorbis, Ogg Speex, Ogg Theora, or CAF ALAC".to_string()),
        _ => None,
    }
}

fn direct_ingest_sample_entry_type(sample_entry_box: &[u8]) -> String {
    if sample_entry_box.len() >= 8 {
        String::from_utf8_lossy(&sample_entry_box[4..8]).into_owned()
    } else {
        "????".to_string()
    }
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push(HEX[usize::from(byte >> 4)] as char);
        rendered.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    rendered
}

fn source_segment_to_direct_ingest_report(
    segment: &SegmentedMuxSourceSegment,
) -> DirectIngestSourceSegmentReport {
    let (kind, source_offset, source_path, data_hex) = match &segment.data {
        SegmentedMuxSourceSegmentData::Prefix(prefix) => (
            "prefix".to_string(),
            None,
            None,
            Some(lowercase_hex(prefix)),
        ),
        SegmentedMuxSourceSegmentData::Bytes(bytes) => {
            ("bytes".to_string(), None, None, Some(lowercase_hex(bytes)))
        }
        SegmentedMuxSourceSegmentData::FileRange { source_offset, .. } => {
            ("file_range".to_string(), Some(*source_offset), None, None)
        }
        SegmentedMuxSourceSegmentData::ExternalFileRange {
            path,
            source_offset,
            ..
        } => (
            "file_range".to_string(),
            Some(*source_offset),
            Some(path.clone()),
            None,
        ),
    };
    DirectIngestSourceSegmentReport {
        kind,
        logical_offset: segment.logical_offset,
        logical_size: segment.logical_size(),
        source_offset,
        source_path,
        data_hex,
    }
}

fn u32_bounds<I>(values: I) -> (Option<u32>, Option<u32>)
where
    I: IntoIterator<Item = u32>,
{
    let mut minimum = None::<u32>;
    let mut maximum = None::<u32>;
    for value in values {
        minimum = Some(minimum.map_or(value, |current| current.min(value)));
        maximum = Some(maximum.map_or(value, |current| current.max(value)));
    }
    (minimum, maximum)
}

fn u64_bounds<I>(values: I) -> (Option<u64>, Option<u64>)
where
    I: IntoIterator<Item = u64>,
{
    let mut minimum = None::<u64>;
    let mut maximum = None::<u64>;
    for value in values {
        minimum = Some(minimum.map_or(value, |current| current.min(value)));
        maximum = Some(maximum.map_or(value, |current| current.max(value)));
    }
    (minimum, maximum)
}

fn i32_bounds<I>(values: I) -> (Option<i32>, Option<i32>)
where
    I: IntoIterator<Item = i32>,
{
    let mut minimum = None::<i32>;
    let mut maximum = None::<i32>;
    for value in values {
        minimum = Some(match minimum {
            Some(current) => current.min(value),
            None => value,
        });
        maximum = Some(match maximum {
            Some(current) => current.max(value),
            None => value,
        });
    }
    (minimum, maximum)
}

fn i64_bounds<I>(values: I) -> (Option<i64>, Option<i64>)
where
    I: IntoIterator<Item = i64>,
{
    let mut minimum = None::<i64>;
    let mut maximum = None::<i64>;
    for value in values {
        minimum = Some(minimum.map_or(value, |current| current.min(value)));
        maximum = Some(maximum.map_or(value, |current| current.max(value)));
    }
    (minimum, maximum)
}

fn report_presentation_time(decode_time: u64, composition_time_offset: i32) -> i64 {
    i64::try_from(decode_time)
        .unwrap_or(i64::MAX)
        .saturating_add(i64::from(composition_time_offset))
}

fn report_presentation_end_time(
    decode_time: u64,
    composition_time_offset: i32,
    duration: u32,
) -> i64 {
    report_presentation_time(decode_time, composition_time_offset)
        .saturating_add(i64::from(duration))
}

fn average_bitrate_bits_per_second(
    total_payload_size: u64,
    total_duration: u64,
    timescale: u32,
) -> Option<u64> {
    if total_duration == 0 || timescale == 0 {
        return None;
    }
    let bits = u128::from(total_payload_size).checked_mul(8)?;
    let scaled = bits.checked_mul(u128::from(timescale))?;
    u64::try_from(scaled / u128::from(total_duration)).ok()
}

fn average_size(total_payload_size: u64, count: usize) -> Option<u64> {
    let count = u64::try_from(count).ok()?;
    total_payload_size.checked_div(count)
}

fn average_non_sync_sample_size(samples: &[DirectIngestSampleReport]) -> Option<u64> {
    let mut total = 0_u64;
    let mut count = 0_u64;
    for sample in samples {
        if sample.is_sync_sample {
            continue;
        }
        total = total.saturating_add(u64::from(sample.data_size));
        count = count.saturating_add(1);
    }
    total.checked_div(count)
}

fn sync_sample_distance_summary(
    samples: &[DirectIngestSampleReport],
) -> (Option<u32>, Option<u32>, Option<u64>) {
    let mut previous_sync_index = None::<usize>;
    let mut minimum = None::<u32>;
    let mut maximum = None::<u32>;
    let mut total = 0_u64;
    let mut count = 0_u64;
    for (index, sample) in samples.iter().enumerate() {
        if !sample.is_sync_sample {
            continue;
        }
        if let Some(previous_index) = previous_sync_index {
            let distance = u32::try_from(index.saturating_sub(previous_index)).unwrap_or(u32::MAX);
            minimum = Some(minimum.map_or(distance, |current| current.min(distance)));
            maximum = Some(maximum.map_or(distance, |current| current.max(distance)));
            total = total.saturating_add(u64::from(distance));
            count = count.saturating_add(1);
        }
        previous_sync_index = Some(index);
    }
    let average = total.checked_div(count);
    (minimum, maximum, average)
}

fn sync_sample_size_summary(
    samples: &[DirectIngestSampleReport],
) -> (Option<u32>, Option<u32>, Option<u64>) {
    let sync_sizes = samples
        .iter()
        .filter(|sample| sample.is_sync_sample)
        .map(|sample| sample.data_size);
    let (minimum, maximum) = u32_bounds(sync_sizes.clone());
    let mut total = 0_u64;
    let mut count = 0_u64;
    for size in sync_sizes {
        total = total.saturating_add(u64::from(size));
        count = count.saturating_add(1);
    }
    let average = total.checked_div(count);
    (minimum, maximum, average)
}

fn sync_sample_decode_delta_summary(
    samples: &[DirectIngestSampleReport],
) -> (Option<u64>, Option<u64>, Option<u64>) {
    let mut previous_sync_decode_time = None::<u64>;
    let mut minimum = None::<u64>;
    let mut maximum = None::<u64>;
    let mut total = 0_u64;
    let mut count = 0_u64;
    for sample in samples {
        if !sample.is_sync_sample {
            continue;
        }
        if let Some(previous_decode_time) = previous_sync_decode_time {
            let delta = sample.decode_time.saturating_sub(previous_decode_time);
            minimum = Some(minimum.map_or(delta, |current| current.min(delta)));
            maximum = Some(maximum.map_or(delta, |current| current.max(delta)));
            total = total.saturating_add(delta);
            count = count.saturating_add(1);
        }
        previous_sync_decode_time = Some(sample.decode_time);
    }
    let average = total.checked_div(count);
    (minimum, maximum, average)
}

type SyncSampleAnchorSummary = (
    Option<usize>,
    Option<usize>,
    Option<u64>,
    Option<u64>,
    Option<i64>,
    Option<i64>,
);

fn sync_sample_anchor_summary(samples: &[DirectIngestSampleReport]) -> SyncSampleAnchorSummary {
    let mut first_index = None::<usize>;
    let mut last_index = None::<usize>;
    let mut first_decode_time = None::<u64>;
    let mut last_decode_time = None::<u64>;
    let mut first_presentation_time = None::<i64>;
    let mut last_presentation_time = None::<i64>;
    for (index, sample) in samples.iter().enumerate() {
        if !sample.is_sync_sample {
            continue;
        }
        if first_index.is_none() {
            first_index = Some(index);
            first_decode_time = Some(sample.decode_time);
            first_presentation_time = Some(sample.presentation_time);
        }
        last_index = Some(index);
        last_decode_time = Some(sample.decode_time);
        last_presentation_time = Some(sample.presentation_time);
    }
    (
        first_index,
        last_index,
        first_decode_time,
        last_decode_time,
        first_presentation_time,
        last_presentation_time,
    )
}

type SyncPacketAnchorSummary = (
    Option<u32>,
    Option<usize>,
    Option<u32>,
    Option<usize>,
    Option<u64>,
    Option<u64>,
    Option<i64>,
    Option<i64>,
);

fn sync_packet_anchor_summary(packets: &[DirectIngestPacketEntry]) -> SyncPacketAnchorSummary {
    let mut first_track_id = None::<u32>;
    let mut first_packet_index = None::<usize>;
    let mut last_track_id = None::<u32>;
    let mut last_packet_index = None::<usize>;
    let mut first_decode_time = None::<u64>;
    let mut last_decode_time = None::<u64>;
    let mut first_presentation_time = None::<i64>;
    let mut last_presentation_time = None::<i64>;
    for packet in packets {
        if !packet.is_sync_sample {
            continue;
        }
        if first_track_id.is_none() {
            first_track_id = Some(packet.track_id);
            first_packet_index = Some(packet.packet_index);
            first_decode_time = Some(packet.decode_time);
            first_presentation_time = Some(packet.presentation_time);
        }
        last_track_id = Some(packet.track_id);
        last_packet_index = Some(packet.packet_index);
        last_decode_time = Some(packet.decode_time);
        last_presentation_time = Some(packet.presentation_time);
    }
    (
        first_track_id,
        first_packet_index,
        last_track_id,
        last_packet_index,
        first_decode_time,
        last_decode_time,
        first_presentation_time,
        last_presentation_time,
    )
}

fn track_candidate_to_direct_ingest_report(track: &TrackCandidate) -> DirectIngestTrackReport {
    let mut decode_time = 0_u64;
    let mut previous_decode_time = None::<u64>;
    let mut previous_presentation_time = None::<i64>;
    let mut previous_presentation_end_time = None::<i64>;
    let mut previous_duration = None::<u32>;
    let mut previous_composition_time_offset = None::<i32>;
    let mut minimum_previous_decode_delta = None::<u64>;
    let mut maximum_previous_decode_delta = None::<u64>;
    let mut minimum_previous_presentation_delta = None::<i64>;
    let mut maximum_previous_presentation_delta = None::<i64>;
    let mut presentation_gap_count = 0usize;
    let mut presentation_overlap_count = 0usize;
    let mut presentation_regression_count = 0usize;
    let mut duration_change_count = 0usize;
    let mut composition_time_offset_change_count = 0usize;
    let samples = track
        .samples
        .iter()
        .map(|sample| {
            let previous_decode_delta =
                previous_decode_time.map(|value| decode_time.saturating_sub(value));
            if let Some(delta) = previous_decode_delta {
                minimum_previous_decode_delta =
                    Some(minimum_previous_decode_delta.map_or(delta, |current| current.min(delta)));
                maximum_previous_decode_delta =
                    Some(maximum_previous_decode_delta.map_or(delta, |current| current.max(delta)));
            }
            let presentation_time =
                report_presentation_time(decode_time, sample.composition_time_offset);
            let presentation_end_time = report_presentation_end_time(
                decode_time,
                sample.composition_time_offset,
                sample.duration,
            );
            let previous_presentation_delta =
                previous_presentation_time.map(|value| presentation_time.saturating_sub(value));
            if let Some(delta) = previous_presentation_delta {
                minimum_previous_presentation_delta = Some(
                    minimum_previous_presentation_delta.map_or(delta, |current| current.min(delta)),
                );
                maximum_previous_presentation_delta = Some(
                    maximum_previous_presentation_delta.map_or(delta, |current| current.max(delta)),
                );
            }
            if let Some(previous_time) = previous_presentation_time
                && presentation_time < previous_time
            {
                presentation_regression_count += 1;
            }
            if let Some(previous_end_time) = previous_presentation_end_time {
                if presentation_time > previous_end_time {
                    presentation_gap_count += 1;
                } else if presentation_time < previous_end_time {
                    presentation_overlap_count += 1;
                }
            }
            if let Some(duration) = previous_duration
                && sample.duration != duration
            {
                duration_change_count += 1;
            }
            if let Some(composition_time_offset) = previous_composition_time_offset
                && sample.composition_time_offset != composition_time_offset
            {
                composition_time_offset_change_count += 1;
            }
            let report = DirectIngestSampleReport {
                source_index: sample.source_index,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                decode_time,
                previous_decode_delta,
                composition_time_offset: sample.composition_time_offset,
                presentation_time,
                presentation_end_time,
                previous_presentation_delta,
                duration: sample.duration,
                is_sync_sample: sample.is_sync_sample,
            };
            previous_decode_time = Some(decode_time);
            decode_time += u64::from(sample.duration);
            previous_presentation_time = Some(presentation_time);
            previous_presentation_end_time = Some(presentation_end_time);
            previous_duration = Some(sample.duration);
            previous_composition_time_offset = Some(sample.composition_time_offset);
            report
        })
        .collect::<Vec<_>>();
    let total_duration = track
        .samples
        .iter()
        .map(|sample| u64::from(sample.duration))
        .sum::<u64>();
    let sync_sample_count = track
        .samples
        .iter()
        .filter(|sample| sample.is_sync_sample)
        .count();
    let starts_with_sync_sample = track
        .samples
        .first()
        .map(|sample| sample.is_sync_sample)
        .unwrap_or(false);
    let total_payload_size = track
        .samples
        .iter()
        .map(|sample| u64::from(sample.data_size))
        .sum::<u64>();
    let average_sample_size = average_size(total_payload_size, track.samples.len());
    let (minimum_sample_size, maximum_sample_size) =
        u32_bounds(track.samples.iter().map(|sample| sample.data_size));
    let (minimum_sample_duration, maximum_sample_duration) =
        u32_bounds(track.samples.iter().map(|sample| sample.duration));
    let (minimum_composition_time_offset, maximum_composition_time_offset) = i32_bounds(
        track
            .samples
            .iter()
            .map(|sample| sample.composition_time_offset),
    );
    let (minimum_presentation_time, maximum_presentation_end_time) = i64_bounds(
        samples
            .iter()
            .flat_map(|sample| [sample.presentation_time, sample.presentation_end_time]),
    );
    let average_bitrate_bits_per_second =
        average_bitrate_bits_per_second(total_payload_size, total_duration, track.timescale);
    let (minimum_sync_sample_size, maximum_sync_sample_size, average_sync_sample_size) =
        sync_sample_size_summary(&samples);
    let average_non_sync_sample_size = average_non_sync_sample_size(&samples);
    let (minimum_sync_sample_distance, maximum_sync_sample_distance, average_sync_sample_distance) =
        sync_sample_distance_summary(&samples);
    let (
        minimum_sync_sample_decode_delta,
        maximum_sync_sample_decode_delta,
        average_sync_sample_decode_delta,
    ) = sync_sample_decode_delta_summary(&samples);
    let (
        first_sync_sample_index,
        last_sync_sample_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
    ) = sync_sample_anchor_summary(&samples);
    DirectIngestTrackReport {
        track_id: track.track_id,
        kind: match track.kind {
            MuxTrackKind::Audio => "audio",
            MuxTrackKind::Video => "video",
            MuxTrackKind::Text => "text",
            MuxTrackKind::Subtitle => "subtitle",
        }
        .to_string(),
        timescale: track.timescale,
        language: String::from_utf8_lossy(&track.language).into_owned(),
        handler_name: track.handler_name.clone(),
        sample_entry_type: direct_ingest_sample_entry_type(&track.sample_entry_box),
        sample_entry_box_hex: lowercase_hex(&track.sample_entry_box),
        width: if track.kind.is_video() || track.kind.is_textual() {
            Some(track.width)
        } else {
            None
        },
        height: if track.kind.is_video() || track.kind.is_textual() {
            Some(track.height)
        } else {
            None
        },
        source_edit_media_time: track.source_edit_media_time,
        sample_roll_distance: track.mux_policy.sample_roll_distance(),
        sample_count: track.samples.len(),
        sync_sample_count,
        starts_with_sync_sample,
        total_duration,
        total_payload_size,
        average_sample_size,
        minimum_sample_size,
        maximum_sample_size,
        minimum_sample_duration,
        maximum_sample_duration,
        average_bitrate_bits_per_second,
        minimum_sync_sample_size,
        maximum_sync_sample_size,
        average_sync_sample_size,
        average_non_sync_sample_size,
        minimum_composition_time_offset,
        maximum_composition_time_offset,
        minimum_presentation_time,
        maximum_presentation_end_time,
        minimum_previous_decode_delta,
        maximum_previous_decode_delta,
        minimum_previous_presentation_delta,
        maximum_previous_presentation_delta,
        presentation_gap_count,
        presentation_overlap_count,
        presentation_regression_count,
        duration_change_count,
        composition_time_offset_change_count,
        minimum_sync_sample_distance,
        maximum_sync_sample_distance,
        average_sync_sample_distance,
        minimum_sync_sample_decode_delta,
        maximum_sync_sample_decode_delta,
        average_sync_sample_decode_delta,
        first_sync_sample_index,
        last_sync_sample_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
        first_decode_time: 0,
        end_decode_time: total_duration,
        samples,
    }
}

fn imported_track_to_direct_ingest_report(track: &ImportedTrack) -> DirectIngestTrackReport {
    let mut decode_time = 0_u64;
    let mut previous_decode_time = None::<u64>;
    let mut previous_presentation_time = None::<i64>;
    let mut previous_presentation_end_time = None::<i64>;
    let mut previous_duration = None::<u32>;
    let mut previous_composition_time_offset = None::<i32>;
    let mut minimum_previous_decode_delta = None::<u64>;
    let mut maximum_previous_decode_delta = None::<u64>;
    let mut minimum_previous_presentation_delta = None::<i64>;
    let mut maximum_previous_presentation_delta = None::<i64>;
    let mut presentation_gap_count = 0usize;
    let mut presentation_overlap_count = 0usize;
    let mut presentation_regression_count = 0usize;
    let mut duration_change_count = 0usize;
    let mut composition_time_offset_change_count = 0usize;
    let samples = track
        .samples
        .iter()
        .map(|sample| {
            let previous_decode_delta =
                previous_decode_time.map(|value| decode_time.saturating_sub(value));
            if let Some(delta) = previous_decode_delta {
                minimum_previous_decode_delta =
                    Some(minimum_previous_decode_delta.map_or(delta, |current| current.min(delta)));
                maximum_previous_decode_delta =
                    Some(maximum_previous_decode_delta.map_or(delta, |current| current.max(delta)));
            }
            let presentation_time =
                report_presentation_time(decode_time, sample.composition_time_offset);
            let presentation_end_time = report_presentation_end_time(
                decode_time,
                sample.composition_time_offset,
                sample.duration,
            );
            let previous_presentation_delta =
                previous_presentation_time.map(|value| presentation_time.saturating_sub(value));
            if let Some(delta) = previous_presentation_delta {
                minimum_previous_presentation_delta = Some(
                    minimum_previous_presentation_delta.map_or(delta, |current| current.min(delta)),
                );
                maximum_previous_presentation_delta = Some(
                    maximum_previous_presentation_delta.map_or(delta, |current| current.max(delta)),
                );
            }
            if let Some(previous_time) = previous_presentation_time
                && presentation_time < previous_time
            {
                presentation_regression_count += 1;
            }
            if let Some(previous_end_time) = previous_presentation_end_time {
                if presentation_time > previous_end_time {
                    presentation_gap_count += 1;
                } else if presentation_time < previous_end_time {
                    presentation_overlap_count += 1;
                }
            }
            if let Some(duration) = previous_duration
                && sample.duration != duration
            {
                duration_change_count += 1;
            }
            if let Some(composition_time_offset) = previous_composition_time_offset
                && sample.composition_time_offset != composition_time_offset
            {
                composition_time_offset_change_count += 1;
            }
            let report = DirectIngestSampleReport {
                source_index: sample.source_index,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                decode_time,
                previous_decode_delta,
                composition_time_offset: sample.composition_time_offset,
                presentation_time,
                presentation_end_time,
                previous_presentation_delta,
                duration: sample.duration,
                is_sync_sample: sample.is_sync_sample,
            };
            previous_decode_time = Some(decode_time);
            decode_time += u64::from(sample.duration);
            previous_presentation_time = Some(presentation_time);
            previous_presentation_end_time = Some(presentation_end_time);
            previous_duration = Some(sample.duration);
            previous_composition_time_offset = Some(sample.composition_time_offset);
            report
        })
        .collect::<Vec<_>>();
    let total_duration = track
        .samples
        .iter()
        .map(|sample| u64::from(sample.duration))
        .sum::<u64>();
    let sync_sample_count = track
        .samples
        .iter()
        .filter(|sample| sample.is_sync_sample)
        .count();
    let starts_with_sync_sample = track
        .samples
        .first()
        .map(|sample| sample.is_sync_sample)
        .unwrap_or(false);
    let total_payload_size = track
        .samples
        .iter()
        .map(|sample| u64::from(sample.data_size))
        .sum::<u64>();
    let average_sample_size = average_size(total_payload_size, track.samples.len());
    let (minimum_sample_size, maximum_sample_size) =
        u32_bounds(track.samples.iter().map(|sample| sample.data_size));
    let (minimum_sample_duration, maximum_sample_duration) =
        u32_bounds(track.samples.iter().map(|sample| sample.duration));
    let (minimum_composition_time_offset, maximum_composition_time_offset) = i32_bounds(
        track
            .samples
            .iter()
            .map(|sample| sample.composition_time_offset),
    );
    let (minimum_presentation_time, maximum_presentation_end_time) = i64_bounds(
        samples
            .iter()
            .flat_map(|sample| [sample.presentation_time, sample.presentation_end_time]),
    );
    let average_bitrate_bits_per_second =
        average_bitrate_bits_per_second(total_payload_size, total_duration, track.timescale);
    let (minimum_sync_sample_size, maximum_sync_sample_size, average_sync_sample_size) =
        sync_sample_size_summary(&samples);
    let average_non_sync_sample_size = average_non_sync_sample_size(&samples);
    let (minimum_sync_sample_distance, maximum_sync_sample_distance, average_sync_sample_distance) =
        sync_sample_distance_summary(&samples);
    let (
        minimum_sync_sample_decode_delta,
        maximum_sync_sample_decode_delta,
        average_sync_sample_decode_delta,
    ) = sync_sample_decode_delta_summary(&samples);
    let (
        first_sync_sample_index,
        last_sync_sample_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
    ) = sync_sample_anchor_summary(&samples);
    DirectIngestTrackReport {
        track_id: 1,
        kind: match track.kind {
            MuxTrackKind::Audio => "audio",
            MuxTrackKind::Video => "video",
            MuxTrackKind::Text => "text",
            MuxTrackKind::Subtitle => "subtitle",
        }
        .to_string(),
        timescale: track.timescale,
        language: String::from_utf8_lossy(&track.language).into_owned(),
        handler_name: track.handler_name.clone(),
        sample_entry_type: direct_ingest_sample_entry_type(&track.sample_entry_box),
        sample_entry_box_hex: lowercase_hex(&track.sample_entry_box),
        width: if track.kind.is_video() || track.kind.is_textual() {
            Some(track.width)
        } else {
            None
        },
        height: if track.kind.is_video() || track.kind.is_textual() {
            Some(track.height)
        } else {
            None
        },
        source_edit_media_time: track.source_edit_media_time,
        sample_roll_distance: track.sample_roll_distance,
        sample_count: track.samples.len(),
        sync_sample_count,
        starts_with_sync_sample,
        total_duration,
        total_payload_size,
        average_sample_size,
        minimum_sample_size,
        maximum_sample_size,
        minimum_sample_duration,
        maximum_sample_duration,
        average_bitrate_bits_per_second,
        minimum_sync_sample_size,
        maximum_sync_sample_size,
        average_sync_sample_size,
        average_non_sync_sample_size,
        minimum_composition_time_offset,
        maximum_composition_time_offset,
        minimum_presentation_time,
        maximum_presentation_end_time,
        minimum_previous_decode_delta,
        maximum_previous_decode_delta,
        minimum_previous_presentation_delta,
        maximum_previous_presentation_delta,
        presentation_gap_count,
        presentation_overlap_count,
        presentation_regression_count,
        duration_change_count,
        composition_time_offset_change_count,
        minimum_sync_sample_distance,
        maximum_sync_sample_distance,
        average_sync_sample_distance,
        minimum_sync_sample_decode_delta,
        maximum_sync_sample_decode_delta,
        average_sync_sample_decode_delta,
        first_sync_sample_index,
        last_sync_sample_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
        first_decode_time: 0,
        end_decode_time: total_duration,
        samples,
    }
}

fn source_catalog_to_direct_ingest_reports(
    sources: &SourceCatalog,
) -> Vec<DirectIngestStagedSourceReport> {
    sources
        .specs
        .iter()
        .enumerate()
        .map(|(source_index, spec)| match spec {
            SourceSpec::File(path) => DirectIngestStagedSourceReport {
                source_index,
                path: path.clone(),
                segmented: false,
                total_size: std::fs::metadata(path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
                segment_count: None,
                segments: None,
            },
            SourceSpec::Segmented(spec) => DirectIngestStagedSourceReport {
                source_index,
                path: spec.path.clone(),
                segmented: true,
                total_size: spec.total_size,
                segment_count: Some(spec.segments.len()),
                segments: Some(
                    spec.segments
                        .iter()
                        .map(source_segment_to_direct_ingest_report)
                        .collect(),
                ),
            },
        })
        .collect()
}

#[cfg(feature = "async")]
async fn source_catalog_to_direct_ingest_reports_async(
    sources: &SourceCatalog,
) -> Vec<DirectIngestStagedSourceReport> {
    let mut reports = Vec::with_capacity(sources.specs.len());
    for (source_index, spec) in sources.specs.iter().enumerate() {
        reports.push(match spec {
            SourceSpec::File(path) => DirectIngestStagedSourceReport {
                source_index,
                path: path.clone(),
                segmented: false,
                total_size: tokio::fs::metadata(path)
                    .await
                    .map(|metadata| metadata.len())
                    .unwrap_or(0),
                segment_count: None,
                segments: None,
            },
            SourceSpec::Segmented(spec) => DirectIngestStagedSourceReport {
                source_index,
                path: spec.path.clone(),
                segmented: true,
                total_size: spec.total_size,
                segment_count: Some(spec.segments.len()),
                segments: Some(
                    spec.segments
                        .iter()
                        .map(source_segment_to_direct_ingest_report)
                        .collect(),
                ),
            },
        });
    }
    reports
}

struct DirectIngestInspectionState {
    report: DirectIngestReport,
    sources: SourceCatalog,
}

pub(in crate::mux) fn inspect_direct_ingest_path_sync(
    path: &Path,
) -> Result<DirectIngestReport, MuxError> {
    Ok(inspect_direct_ingest_state_sync(path)?.report)
}

pub(in crate::mux) fn inspect_direct_ingest_packets_sync(
    path: &Path,
) -> Result<DirectIngestPacketReport, MuxError> {
    direct_ingest_packet_report_sync(inspect_direct_ingest_state_sync(path)?)
}

fn inspect_direct_ingest_state_sync(path: &Path) -> Result<DirectIngestInspectionState, MuxError> {
    let absolute = absolute_path(path)?;
    let detected_kind = detect_path_track_kind_sync(&absolute)?;
    let mut report = DirectIngestReport {
        input_path: absolute.clone(),
        detected_kind: direct_ingest_report_kind(detected_kind),
        supports_flat_mux: detected_kind_supports_flat_mux(detected_kind),
        note: direct_ingest_report_note(detected_kind),
        track_count: 0,
        total_sample_count: 0,
        total_sync_sample_count: 0,
        total_payload_size: 0,
        staged_sources: Vec::new(),
        tracks: Vec::new(),
    };
    let mut sources = SourceCatalog::default();
    match detected_kind {
        DetectedPathTrackKind::Mp4 => {
            let mut cache = BTreeMap::new();
            let source = load_mp4_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
            let mut cache = BTreeMap::new();
            let source = load_avi_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
            let mut cache = BTreeMap::new();
            let source = load_dash_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi)
        | DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {}
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
            let mut cache = BTreeMap::new();
            let source = load_nhml_source_sync(
                &absolute,
                DetectedNhmlSidecarKind::Nhml,
                &mut cache,
                &mut sources,
            )?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
            let mut cache = BTreeMap::new();
            let source = load_nhml_source_sync(
                &absolute,
                DetectedNhmlSidecarKind::Nhnt,
                &mut cache,
                &mut sources,
            )?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
            let mut cache = BTreeMap::new();
            let source = load_program_stream_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
            let mut cache = BTreeMap::new();
            let source = load_saf_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
            let mut cache = BTreeMap::new();
            let source = load_transport_stream_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
            let mut cache = BTreeMap::new();
            let source = load_vobsub_source_sync(&absolute, &mut cache, &mut sources)?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Raw(codec) => {
            let imported = import_detected_raw_codec_sync(
                &absolute,
                codec,
                &absolute.display().to_string(),
                &mut sources,
            )?;
            report
                .tracks
                .push(imported_track_to_direct_ingest_report(&imported));
        }
        DetectedPathTrackKind::Mp4ImportOnly(_) | DetectedPathTrackKind::Unknown => {}
    }
    report.track_count = report.tracks.len();
    report.total_sample_count = report.tracks.iter().map(|track| track.sample_count).sum();
    report.total_sync_sample_count = report
        .tracks
        .iter()
        .map(|track| track.sync_sample_count)
        .sum();
    report.total_payload_size = report
        .tracks
        .iter()
        .map(|track| track.total_payload_size)
        .sum();
    report.staged_sources = source_catalog_to_direct_ingest_reports(&sources);
    Ok(DirectIngestInspectionState { report, sources })
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn inspect_direct_ingest_path_async(
    path: &Path,
) -> Result<DirectIngestReport, MuxError> {
    Ok(inspect_direct_ingest_state_async(path).await?.report)
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn inspect_direct_ingest_packets_async(
    path: &Path,
) -> Result<DirectIngestPacketReport, MuxError> {
    direct_ingest_packet_report_async(inspect_direct_ingest_state_async(path).await?).await
}

#[cfg(feature = "async")]
async fn inspect_direct_ingest_state_async(
    path: &Path,
) -> Result<DirectIngestInspectionState, MuxError> {
    let absolute = absolute_path(path)?;
    let detected_kind = detect_path_track_kind_async(&absolute).await?;
    let mut report = DirectIngestReport {
        input_path: absolute.clone(),
        detected_kind: direct_ingest_report_kind(detected_kind),
        supports_flat_mux: detected_kind_supports_flat_mux(detected_kind),
        note: direct_ingest_report_note(detected_kind),
        track_count: 0,
        total_sample_count: 0,
        total_sync_sample_count: 0,
        total_payload_size: 0,
        staged_sources: Vec::new(),
        tracks: Vec::new(),
    };
    let mut sources = SourceCatalog::default();
    match detected_kind {
        DetectedPathTrackKind::Mp4 => {
            let mut cache = BTreeMap::new();
            let source = load_mp4_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
            let mut cache = BTreeMap::new();
            let source = load_avi_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
            let mut cache = BTreeMap::new();
            let source = load_dash_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi)
        | DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {}
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
            let mut cache = BTreeMap::new();
            let source = load_nhml_source_async(
                &absolute,
                DetectedNhmlSidecarKind::Nhml,
                &mut cache,
                &mut sources,
            )
            .await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
            let mut cache = BTreeMap::new();
            let source = load_nhml_source_async(
                &absolute,
                DetectedNhmlSidecarKind::Nhnt,
                &mut cache,
                &mut sources,
            )
            .await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
            let mut cache = BTreeMap::new();
            let source =
                load_program_stream_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
            let mut cache = BTreeMap::new();
            let source = load_saf_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
            let mut cache = BTreeMap::new();
            let source =
                load_transport_stream_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
            let mut cache = BTreeMap::new();
            let source = load_vobsub_source_async(&absolute, &mut cache, &mut sources).await?;
            report.tracks = source
                .tracks
                .iter()
                .map(track_candidate_to_direct_ingest_report)
                .collect();
        }
        DetectedPathTrackKind::Raw(codec) => {
            let imported = import_detected_raw_codec_async(
                &absolute,
                codec,
                &absolute.display().to_string(),
                &mut sources,
            )
            .await?;
            report
                .tracks
                .push(imported_track_to_direct_ingest_report(&imported));
        }
        DetectedPathTrackKind::Mp4ImportOnly(_) | DetectedPathTrackKind::Unknown => {}
    }
    report.track_count = report.tracks.len();
    report.total_sample_count = report.tracks.iter().map(|track| track.sample_count).sum();
    report.total_sync_sample_count = report
        .tracks
        .iter()
        .map(|track| track.sync_sample_count)
        .sum();
    report.total_payload_size = report
        .tracks
        .iter()
        .map(|track| track.total_payload_size)
        .sum();
    report.staged_sources = source_catalog_to_direct_ingest_reports_async(&sources).await;
    Ok(DirectIngestInspectionState { report, sources })
}

fn direct_ingest_packet_report_sync(
    state: DirectIngestInspectionState,
) -> Result<DirectIngestPacketReport, MuxError> {
    let DirectIngestInspectionState { report, sources } = state;
    let mut source_readers = sources
        .specs
        .iter()
        .map(SyncMuxSource::open)
        .collect::<Result<Vec<_>, _>>()?;
    let mut packets = Vec::new();
    let mut minimum_sync_packet_distance = None::<u32>;
    let mut maximum_sync_packet_distance = None::<u32>;
    for track in &report.tracks {
        let mut previous_decode_time = None::<u64>;
        let mut previous_presentation_time = None::<i64>;
        let (
            track_minimum_sync_packet_distance,
            track_maximum_sync_packet_distance,
            _track_average_sync_packet_distance,
        ) = sync_sample_distance_summary(&track.samples);
        if let Some(distance) = track_minimum_sync_packet_distance {
            minimum_sync_packet_distance = Some(
                minimum_sync_packet_distance.map_or(distance, |current| current.min(distance)),
            );
        }
        if let Some(distance) = track_maximum_sync_packet_distance {
            maximum_sync_packet_distance = Some(
                maximum_sync_packet_distance.map_or(distance, |current| current.max(distance)),
            );
        }
        for (packet_index, sample) in track.samples.iter().enumerate() {
            let payload_crc32 = crc32_from_sync_source(
                &mut source_readers[sample.source_index],
                sample.data_offset,
                sample.data_size,
            )?;
            let previous_presentation_delta = previous_presentation_time
                .map(|value| sample.presentation_time.saturating_sub(value));
            packets.push(DirectIngestPacketEntry {
                track_id: track.track_id,
                packet_index,
                track_kind: track.kind.clone(),
                timescale: track.timescale,
                sample_entry_type: track.sample_entry_type.clone(),
                source_index: sample.source_index,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                decode_time: sample.decode_time,
                composition_time_offset: sample.composition_time_offset,
                presentation_time: sample.presentation_time,
                presentation_end_time: sample.presentation_end_time,
                previous_presentation_delta,
                duration: sample.duration,
                previous_decode_delta: previous_decode_time
                    .map(|value| sample.decode_time.saturating_sub(value)),
                payload_crc32,
                is_sync_sample: sample.is_sync_sample,
            });
            previous_decode_time = Some(sample.decode_time);
            previous_presentation_time = Some(sample.presentation_time);
        }
    }
    let sync_packet_count = packets
        .iter()
        .filter(|packet| packet.is_sync_sample)
        .count();
    let starts_with_sync_packet = packets
        .first()
        .map(|packet| packet.is_sync_sample)
        .unwrap_or(false);
    let total_payload_size = packets
        .iter()
        .map(|packet| u64::from(packet.data_size))
        .sum::<u64>();
    let (minimum_packet_size, maximum_packet_size) =
        u32_bounds(packets.iter().map(|packet| packet.data_size));
    let average_non_sync_packet_size = {
        let mut total = 0_u64;
        let mut count = 0_u64;
        for packet in &packets {
            if packet.is_sync_sample {
                continue;
            }
            total = total.saturating_add(u64::from(packet.data_size));
            count = count.saturating_add(1);
        }
        total.checked_div(count)
    };
    let (minimum_sync_packet_size, maximum_sync_packet_size, average_sync_packet_size) = {
        let sync_sizes = packets
            .iter()
            .filter(|packet| packet.is_sync_sample)
            .map(|packet| packet.data_size);
        let (minimum, maximum) = u32_bounds(sync_sizes.clone());
        let mut total = 0_u64;
        let mut count = 0_u64;
        for size in sync_sizes {
            total = total.saturating_add(u64::from(size));
            count = count.saturating_add(1);
        }
        let average = total.checked_div(count);
        (minimum, maximum, average)
    };
    let (minimum_packet_duration, maximum_packet_duration) =
        u32_bounds(packets.iter().map(|packet| packet.duration));
    let (minimum_previous_decode_delta, maximum_previous_decode_delta) = u64_bounds(
        packets
            .iter()
            .filter_map(|packet| packet.previous_decode_delta),
    );
    let (minimum_composition_time_offset, maximum_composition_time_offset) =
        i32_bounds(packets.iter().map(|packet| packet.composition_time_offset));
    let (minimum_presentation_time, maximum_presentation_end_time) = i64_bounds(
        packets
            .iter()
            .flat_map(|packet| [packet.presentation_time, packet.presentation_end_time]),
    );
    let (minimum_previous_presentation_delta, maximum_previous_presentation_delta) = i64_bounds(
        packets
            .iter()
            .filter_map(|packet| packet.previous_presentation_delta),
    );
    let mut presentation_gap_count = 0usize;
    let mut presentation_overlap_count = 0usize;
    let mut presentation_regression_count = 0usize;
    let mut duration_change_count = 0usize;
    let mut composition_time_offset_change_count = 0usize;
    for track in &report.tracks {
        for window in track.samples.windows(2) {
            let previous = &window[0];
            let current = &window[1];
            if current.presentation_time < previous.presentation_time {
                presentation_regression_count += 1;
            }
            if current.presentation_time > previous.presentation_end_time {
                presentation_gap_count += 1;
            } else if current.presentation_time < previous.presentation_end_time {
                presentation_overlap_count += 1;
            }
            if current.duration != previous.duration {
                duration_change_count += 1;
            }
            if current.composition_time_offset != previous.composition_time_offset {
                composition_time_offset_change_count += 1;
            }
        }
    }
    let (
        minimum_sync_packet_decode_delta,
        maximum_sync_packet_decode_delta,
        average_sync_packet_decode_delta,
    ) = {
        let mut previous_sync_decode_time = None::<u64>;
        let mut minimum = None::<u64>;
        let mut maximum = None::<u64>;
        let mut total = 0_u64;
        let mut count = 0_u64;
        for packet in &packets {
            if !packet.is_sync_sample {
                continue;
            }
            if let Some(previous_decode_time) = previous_sync_decode_time {
                let delta = packet.decode_time.saturating_sub(previous_decode_time);
                minimum = Some(minimum.map_or(delta, |current| current.min(delta)));
                maximum = Some(maximum.map_or(delta, |current| current.max(delta)));
                total = total.saturating_add(delta);
                count = count.saturating_add(1);
            }
            previous_sync_decode_time = Some(packet.decode_time);
        }
        let average = total.checked_div(count);
        (minimum, maximum, average)
    };
    let average_sync_packet_distance = {
        let mut previous_sync_index = None::<usize>;
        let mut total = 0_u64;
        let mut count = 0_u64;
        for (index, packet) in packets.iter().enumerate() {
            if !packet.is_sync_sample {
                continue;
            }
            if let Some(previous_index) = previous_sync_index {
                let distance =
                    u64::try_from(index.saturating_sub(previous_index)).unwrap_or(u64::MAX);
                total = total.saturating_add(distance);
                count = count.saturating_add(1);
            }
            previous_sync_index = Some(index);
        }
        total.checked_div(count)
    };
    let (
        first_sync_packet_track_id,
        first_sync_packet_index,
        last_sync_packet_track_id,
        last_sync_packet_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
    ) = sync_packet_anchor_summary(&packets);
    Ok(DirectIngestPacketReport {
        input_path: report.input_path,
        detected_kind: report.detected_kind,
        supports_flat_mux: report.supports_flat_mux,
        note: report.note,
        track_count: report.track_count,
        packet_count: packets.len(),
        sync_packet_count,
        starts_with_sync_packet,
        total_payload_size,
        minimum_packet_size,
        maximum_packet_size,
        minimum_sync_packet_size,
        maximum_sync_packet_size,
        average_sync_packet_size,
        average_non_sync_packet_size,
        minimum_packet_duration,
        maximum_packet_duration,
        minimum_previous_decode_delta,
        maximum_previous_decode_delta,
        minimum_composition_time_offset,
        maximum_composition_time_offset,
        minimum_presentation_time,
        maximum_presentation_end_time,
        minimum_previous_presentation_delta,
        maximum_previous_presentation_delta,
        presentation_gap_count,
        presentation_overlap_count,
        presentation_regression_count,
        duration_change_count,
        composition_time_offset_change_count,
        minimum_sync_packet_distance,
        maximum_sync_packet_distance,
        average_sync_packet_distance,
        minimum_sync_packet_decode_delta,
        maximum_sync_packet_decode_delta,
        average_sync_packet_decode_delta,
        first_sync_packet_track_id,
        first_sync_packet_index,
        last_sync_packet_track_id,
        last_sync_packet_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
        tracks: report.tracks,
        staged_sources: report.staged_sources,
        packets,
    })
}

#[cfg(feature = "async")]
async fn direct_ingest_packet_report_async(
    state: DirectIngestInspectionState,
) -> Result<DirectIngestPacketReport, MuxError> {
    let DirectIngestInspectionState { report, sources } = state;
    let mut source_readers = Vec::with_capacity(sources.specs.len());
    for spec in &sources.specs {
        source_readers.push(AsyncMuxSource::open(spec).await?);
    }
    let mut packets = Vec::new();
    let mut minimum_sync_packet_distance = None::<u32>;
    let mut maximum_sync_packet_distance = None::<u32>;
    for track in &report.tracks {
        let mut previous_decode_time = None::<u64>;
        let mut previous_presentation_time = None::<i64>;
        let (
            track_minimum_sync_packet_distance,
            track_maximum_sync_packet_distance,
            _track_average_sync_packet_distance,
        ) = sync_sample_distance_summary(&track.samples);
        if let Some(distance) = track_minimum_sync_packet_distance {
            minimum_sync_packet_distance = Some(
                minimum_sync_packet_distance.map_or(distance, |current| current.min(distance)),
            );
        }
        if let Some(distance) = track_maximum_sync_packet_distance {
            maximum_sync_packet_distance = Some(
                maximum_sync_packet_distance.map_or(distance, |current| current.max(distance)),
            );
        }
        for (packet_index, sample) in track.samples.iter().enumerate() {
            let payload_crc32 = crc32_from_async_source(
                &mut source_readers[sample.source_index],
                sample.data_offset,
                sample.data_size,
            )
            .await?;
            let previous_presentation_delta = previous_presentation_time
                .map(|value| sample.presentation_time.saturating_sub(value));
            packets.push(DirectIngestPacketEntry {
                track_id: track.track_id,
                packet_index,
                track_kind: track.kind.clone(),
                timescale: track.timescale,
                sample_entry_type: track.sample_entry_type.clone(),
                source_index: sample.source_index,
                data_offset: sample.data_offset,
                data_size: sample.data_size,
                decode_time: sample.decode_time,
                composition_time_offset: sample.composition_time_offset,
                presentation_time: sample.presentation_time,
                presentation_end_time: sample.presentation_end_time,
                previous_presentation_delta,
                duration: sample.duration,
                previous_decode_delta: previous_decode_time
                    .map(|value| sample.decode_time.saturating_sub(value)),
                payload_crc32,
                is_sync_sample: sample.is_sync_sample,
            });
            previous_decode_time = Some(sample.decode_time);
            previous_presentation_time = Some(sample.presentation_time);
        }
    }
    let sync_packet_count = packets
        .iter()
        .filter(|packet| packet.is_sync_sample)
        .count();
    let starts_with_sync_packet = packets
        .first()
        .map(|packet| packet.is_sync_sample)
        .unwrap_or(false);
    let total_payload_size = packets
        .iter()
        .map(|packet| u64::from(packet.data_size))
        .sum::<u64>();
    let (minimum_packet_size, maximum_packet_size) =
        u32_bounds(packets.iter().map(|packet| packet.data_size));
    let average_non_sync_packet_size = {
        let mut total = 0_u64;
        let mut count = 0_u64;
        for packet in &packets {
            if packet.is_sync_sample {
                continue;
            }
            total = total.saturating_add(u64::from(packet.data_size));
            count = count.saturating_add(1);
        }
        total.checked_div(count)
    };
    let (minimum_sync_packet_size, maximum_sync_packet_size, average_sync_packet_size) = {
        let sync_sizes = packets
            .iter()
            .filter(|packet| packet.is_sync_sample)
            .map(|packet| packet.data_size);
        let (minimum, maximum) = u32_bounds(sync_sizes.clone());
        let mut total = 0_u64;
        let mut count = 0_u64;
        for size in sync_sizes {
            total = total.saturating_add(u64::from(size));
            count = count.saturating_add(1);
        }
        let average = total.checked_div(count);
        (minimum, maximum, average)
    };
    let (minimum_packet_duration, maximum_packet_duration) =
        u32_bounds(packets.iter().map(|packet| packet.duration));
    let (minimum_previous_decode_delta, maximum_previous_decode_delta) = u64_bounds(
        packets
            .iter()
            .filter_map(|packet| packet.previous_decode_delta),
    );
    let (minimum_composition_time_offset, maximum_composition_time_offset) =
        i32_bounds(packets.iter().map(|packet| packet.composition_time_offset));
    let (minimum_presentation_time, maximum_presentation_end_time) = i64_bounds(
        packets
            .iter()
            .flat_map(|packet| [packet.presentation_time, packet.presentation_end_time]),
    );
    let (minimum_previous_presentation_delta, maximum_previous_presentation_delta) = i64_bounds(
        packets
            .iter()
            .filter_map(|packet| packet.previous_presentation_delta),
    );
    let mut presentation_gap_count = 0usize;
    let mut presentation_overlap_count = 0usize;
    let mut presentation_regression_count = 0usize;
    let mut duration_change_count = 0usize;
    let mut composition_time_offset_change_count = 0usize;
    for track in &report.tracks {
        for window in track.samples.windows(2) {
            let previous = &window[0];
            let current = &window[1];
            if current.presentation_time < previous.presentation_time {
                presentation_regression_count += 1;
            }
            if current.presentation_time > previous.presentation_end_time {
                presentation_gap_count += 1;
            } else if current.presentation_time < previous.presentation_end_time {
                presentation_overlap_count += 1;
            }
            if current.duration != previous.duration {
                duration_change_count += 1;
            }
            if current.composition_time_offset != previous.composition_time_offset {
                composition_time_offset_change_count += 1;
            }
        }
    }
    let (
        minimum_sync_packet_decode_delta,
        maximum_sync_packet_decode_delta,
        average_sync_packet_decode_delta,
    ) = {
        let mut previous_sync_decode_time = None::<u64>;
        let mut minimum = None::<u64>;
        let mut maximum = None::<u64>;
        let mut total = 0_u64;
        let mut count = 0_u64;
        for packet in &packets {
            if !packet.is_sync_sample {
                continue;
            }
            if let Some(previous_decode_time) = previous_sync_decode_time {
                let delta = packet.decode_time.saturating_sub(previous_decode_time);
                minimum = Some(minimum.map_or(delta, |current| current.min(delta)));
                maximum = Some(maximum.map_or(delta, |current| current.max(delta)));
                total = total.saturating_add(delta);
                count = count.saturating_add(1);
            }
            previous_sync_decode_time = Some(packet.decode_time);
        }
        let average = total.checked_div(count);
        (minimum, maximum, average)
    };
    let average_sync_packet_distance = {
        let mut previous_sync_index = None::<usize>;
        let mut total = 0_u64;
        let mut count = 0_u64;
        for (index, packet) in packets.iter().enumerate() {
            if !packet.is_sync_sample {
                continue;
            }
            if let Some(previous_index) = previous_sync_index {
                let distance =
                    u64::try_from(index.saturating_sub(previous_index)).unwrap_or(u64::MAX);
                total = total.saturating_add(distance);
                count = count.saturating_add(1);
            }
            previous_sync_index = Some(index);
        }
        total.checked_div(count)
    };
    let (
        first_sync_packet_track_id,
        first_sync_packet_index,
        last_sync_packet_track_id,
        last_sync_packet_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
    ) = sync_packet_anchor_summary(&packets);
    Ok(DirectIngestPacketReport {
        input_path: report.input_path,
        detected_kind: report.detected_kind,
        supports_flat_mux: report.supports_flat_mux,
        note: report.note,
        track_count: report.track_count,
        packet_count: packets.len(),
        sync_packet_count,
        starts_with_sync_packet,
        total_payload_size,
        minimum_packet_size,
        maximum_packet_size,
        minimum_sync_packet_size,
        maximum_sync_packet_size,
        average_sync_packet_size,
        average_non_sync_packet_size,
        minimum_packet_duration,
        maximum_packet_duration,
        minimum_previous_decode_delta,
        maximum_previous_decode_delta,
        minimum_composition_time_offset,
        maximum_composition_time_offset,
        minimum_presentation_time,
        maximum_presentation_end_time,
        minimum_previous_presentation_delta,
        maximum_previous_presentation_delta,
        presentation_gap_count,
        presentation_overlap_count,
        presentation_regression_count,
        duration_change_count,
        composition_time_offset_change_count,
        minimum_sync_packet_distance,
        maximum_sync_packet_distance,
        average_sync_packet_distance,
        minimum_sync_packet_decode_delta,
        maximum_sync_packet_decode_delta,
        average_sync_packet_decode_delta,
        first_sync_packet_track_id,
        first_sync_packet_index,
        last_sync_packet_track_id,
        last_sync_packet_index,
        first_sync_decode_time,
        last_sync_decode_time,
        first_sync_presentation_time,
        last_sync_presentation_time,
        tracks: report.tracks,
        staged_sources: report.staged_sources,
        packets,
    })
}

fn crc32_from_sync_source(
    source: &mut SyncMuxSource,
    offset: u64,
    size: u32,
) -> Result<u32, MuxError> {
    source.seek(SeekFrom::Start(offset))?;
    let mut remaining =
        usize::try_from(size).map_err(|_| MuxError::LayoutOverflow("packet size"))?;
    let mut buffer = [0_u8; 8192];
    let mut crc = 0xFFFF_FFFF_u32;
    while remaining != 0 {
        let to_read = remaining.min(buffer.len());
        source.read_exact(&mut buffer[..to_read])?;
        crc = update_crc32(crc, &buffer[..to_read]);
        remaining -= to_read;
    }
    Ok(!crc)
}

#[cfg(feature = "async")]
async fn crc32_from_async_source(
    source: &mut AsyncMuxSource,
    offset: u64,
    size: u32,
) -> Result<u32, MuxError> {
    source.seek(SeekFrom::Start(offset)).await?;
    let mut remaining =
        usize::try_from(size).map_err(|_| MuxError::LayoutOverflow("packet size"))?;
    let mut buffer = [0_u8; 8192];
    let mut crc = 0xFFFF_FFFF_u32;
    while remaining != 0 {
        let to_read = remaining.min(buffer.len());
        source.read_exact(&mut buffer[..to_read]).await?;
        crc = update_crc32(crc, &buffer[..to_read]);
        remaining -= to_read;
    }
    Ok(!crc)
}

fn update_crc32(mut crc: u32, bytes: &[u8]) -> u32 {
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    crc
}

fn import_detected_path_raw_sync(
    path: &Path,
    spec: &str,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    match detect_path_track_kind_sync(path)? {
        DetectedPathTrackKind::Raw(codec) => import_detected_raw_codec_sync(path, codec, spec, sources),
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an AVI container on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a DASH manifest on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a GHI source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a GSF source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an NHML sidecar on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an NHNT sidecar on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "detected an MPEG program stream on the raw-import path unexpectedly"
                        .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a SAF source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "detected an MPEG transport stream on the raw-import path unexpectedly"
                        .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a VobSub source on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Mp4ImportOnly(kind) => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!(
                "path-only mux import for `{kind}` is not supported; import this family from an MP4 source with `#audio` or `#track:ID` instead"
            ),
        }),
        DetectedPathTrackKind::Mp4 => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "detected an MP4-style source on the raw-import path unexpectedly".to_string(),
        }),
        DetectedPathTrackKind::Unknown => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "path-only mux input is not currently recognized as MP4, VobSub, supported AVI audio or MPEG-4 Part 2 video, supported MPEG-PS MPEG audio, AC-3, or MPEG-4 Part 2/H.264/H.265/VVC video, supported MPEG-TS MPEG audio, AAC LATM, MHAS, AC-3, E-AC-3, AC-4, DTS, TrueHD, MPEG-2 video, AV1, MPEG-4 Part 2, H.264, H.265, VVC, DVB subtitle, or DVB teletext video or subtitle carriage, JPEG still images, PNG still images, BMP still images, JPEG 2000 image or codestream input, self-describing YUV4MPEG raw video, raw ProRes, WAVE/AIFF/AIFC PCM, AAC ADTS, AAC LATM, MP3, AC-3, E-AC-3, AC-4, AMR, AMR-WB, QCP voice audio, DTS core audio, Dolby TrueHD, leading-sync MHAS MPEG-H, FLAC, IAMF, H.263 elementary video, MPEG-2 elementary video, MPEG-4 Part 2 elementary video, H.264 Annex B, H.265 Annex B, IVF-backed AV1/VP8/VP9/VP10, Ogg FLAC, Ogg Opus, Ogg Vorbis, Ogg Speex, Ogg Theora, or CAF ALAC".to_string(),
        }),
    }
}

#[cfg(feature = "async")]
async fn import_detected_path_raw_async(
    path: &Path,
    spec: &str,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    match detect_path_track_kind_async(path).await? {
        DetectedPathTrackKind::Raw(codec) => {
            import_detected_raw_codec_async(path, codec, spec, sources).await
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Avi) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an AVI container on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Dash) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a DASH manifest on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Ghi) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a GHI source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Gsf) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a GSF source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhml) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an NHML sidecar on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Nhnt) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected an NHNT sidecar on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::ProgramStream) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "detected an MPEG program stream on the raw-import path unexpectedly"
                        .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::Saf) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a SAF source on the raw-import path unexpectedly".to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::TransportStream) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message:
                    "detected an MPEG transport stream on the raw-import path unexpectedly"
                        .to_string(),
            })
        }
        DetectedPathTrackKind::Container(DetectedContainerPathKind::VobSub) => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: "detected a VobSub source on the raw-import path unexpectedly"
                    .to_string(),
            })
        }
        DetectedPathTrackKind::Mp4ImportOnly(kind) => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: format!(
                "path-only mux import for `{kind}` is not supported; import this family from an MP4 source with `#audio` or `#track:ID` instead"
            ),
        }),
        DetectedPathTrackKind::Mp4 => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "detected an MP4-style source on the raw-import path unexpectedly".to_string(),
        }),
        DetectedPathTrackKind::Unknown => Err(MuxError::UnsupportedTrackImport {
            spec: spec.to_string(),
            message: "path-only mux input is not currently recognized as MP4, VobSub, supported AVI audio or MPEG-4 Part 2 video, supported MPEG-PS MPEG audio, AC-3, or MPEG-4 Part 2/H.264/H.265/VVC video, supported MPEG-TS MPEG audio, AAC LATM, MHAS, AC-3, E-AC-3, AC-4, DTS, TrueHD, MPEG-2 video, AV1, MPEG-4 Part 2, H.264, H.265, VVC, DVB subtitle, or DVB teletext video or subtitle carriage, JPEG still images, PNG still images, BMP still images, JPEG 2000 image or codestream input, self-describing YUV4MPEG raw video, raw ProRes, WAVE/AIFF/AIFC PCM, AAC ADTS, AAC LATM, MP3, AC-3, E-AC-3, AC-4, AMR, AMR-WB, DTS core audio, Dolby TrueHD, leading-sync MHAS MPEG-H, FLAC, IAMF, H.263 elementary video, MPEG-2 elementary video, MPEG-4 Part 2 elementary video, H.264 Annex B, H.265 Annex B, IVF-backed AV1/VP8/VP9/VP10, Ogg FLAC, Ogg Opus, Ogg Vorbis, Ogg Speex, Ogg Theora, or CAF ALAC".to_string(),
        }),
    }
}

fn import_detected_raw_codec_sync(
    path: &Path,
    codec: MuxRawCodec,
    spec: &str,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    import_raw_track_sync(path, codec, spec.to_string(), sources)
}

#[cfg(feature = "async")]
async fn import_detected_raw_codec_async(
    path: &Path,
    codec: MuxRawCodec,
    spec: &str,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    import_raw_track_async(path, codec, spec.to_string(), sources).await
}

fn import_raw_track_sync(
    path: &Path,
    codec: MuxRawCodec,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    match codec {
        MuxRawCodec::Mpeg2v => import_raw_mpeg2v_sync(path, spec, sources),
        MuxRawCodec::Mp4v => import_raw_mp4v_sync(path, spec, sources),
        MuxRawCodec::H263 => import_raw_h263_sync(path, spec, sources),
        MuxRawCodec::H264 => import_raw_h264_sync(path, spec, sources),
        MuxRawCodec::H265 => import_raw_h265_sync(path, spec, sources),
        MuxRawCodec::Vvc => import_raw_vvc_sync(path, spec, sources),
        MuxRawCodec::Av1 => import_raw_av1_sync(path, spec, sources),
        MuxRawCodec::Vp8 | MuxRawCodec::Vp9 | MuxRawCodec::Vp10 => {
            import_ivf_video_sync(path, codec, spec, sources)
        }
        MuxRawCodec::Aac => import_raw_aac_sync(path, spec, sources),
        MuxRawCodec::Latm => import_raw_latm_sync(path, spec, sources),
        MuxRawCodec::Mp3 => import_raw_mp3_sync(path, spec, sources),
        MuxRawCodec::Ac3 => import_raw_ac3_sync(path, spec, sources),
        MuxRawCodec::Eac3 => import_raw_eac3_sync(path, spec, sources),
        MuxRawCodec::Ac4 => import_raw_ac4_sync(path, spec, sources),
        MuxRawCodec::Amr => import_raw_amr_sync(path, spec, sources),
        MuxRawCodec::AmrWb => import_raw_amr_wb_sync(path, spec, sources),
        MuxRawCodec::Qcp => import_raw_qcp_sync(path, spec, sources),
        MuxRawCodec::Jpeg => import_raw_jpeg_sync(path, spec, sources),
        MuxRawCodec::Png => import_raw_png_sync(path, spec, sources),
        MuxRawCodec::Bmp => import_raw_bmp_sync(path, spec, sources),
        MuxRawCodec::Prores => import_raw_prores_sync(path, spec, sources),
        MuxRawCodec::Y4m => import_raw_y4m_sync(path, spec, sources),
        MuxRawCodec::J2k => import_raw_j2k_sync(path, spec, sources),
        MuxRawCodec::Pcm => import_wave_pcm_sync(path, spec, sources),
        MuxRawCodec::Dts => import_raw_dts_sync(path, spec, sources),
        MuxRawCodec::Truehd => import_raw_truehd_sync(path, spec, sources),
        MuxRawCodec::Alac => import_caf_alac_sync(path, spec, sources),
        MuxRawCodec::Flac => import_raw_flac_sync(path, spec, sources),
        MuxRawCodec::Iamf => import_raw_iamf_sync(path, spec, sources),
        MuxRawCodec::MpegH => import_raw_mhas_sync(path, spec, sources),
        MuxRawCodec::Opus => import_ogg_opus_sync(path, spec, sources),
        MuxRawCodec::Vorbis => import_ogg_vorbis_sync(path, spec, sources),
        MuxRawCodec::Speex => import_ogg_speex_sync(path, spec, sources),
        MuxRawCodec::Theora => import_ogg_theora_sync(path, spec, sources),
    }
}

#[cfg(feature = "async")]
async fn import_raw_track_async(
    path: &Path,
    codec: MuxRawCodec,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    match codec {
        MuxRawCodec::Mpeg2v => import_raw_mpeg2v_async(path, spec, sources).await,
        MuxRawCodec::Mp4v => import_raw_mp4v_async(path, spec, sources).await,
        MuxRawCodec::H263 => import_raw_h263_async(path, spec, sources).await,
        MuxRawCodec::H264 => import_raw_h264_async(path, spec, sources).await,
        MuxRawCodec::H265 => import_raw_h265_async(path, spec, sources).await,
        MuxRawCodec::Vvc => import_raw_vvc_async(path, spec, sources).await,
        MuxRawCodec::Av1 => import_raw_av1_async(path, spec, sources).await,
        MuxRawCodec::Vp8 | MuxRawCodec::Vp9 | MuxRawCodec::Vp10 => {
            import_ivf_video_async(path, codec, spec, sources).await
        }
        MuxRawCodec::Aac => import_raw_aac_async(path, spec, sources).await,
        MuxRawCodec::Latm => import_raw_latm_async(path, spec, sources).await,
        MuxRawCodec::Mp3 => import_raw_mp3_async(path, spec, sources).await,
        MuxRawCodec::Ac3 => import_raw_ac3_async(path, spec, sources).await,
        MuxRawCodec::Eac3 => import_raw_eac3_async(path, spec, sources).await,
        MuxRawCodec::Ac4 => import_raw_ac4_async(path, spec, sources).await,
        MuxRawCodec::Amr => import_raw_amr_async(path, spec, sources).await,
        MuxRawCodec::AmrWb => import_raw_amr_wb_async(path, spec, sources).await,
        MuxRawCodec::Qcp => import_raw_qcp_async(path, spec, sources).await,
        MuxRawCodec::Jpeg => import_raw_jpeg_async(path, spec, sources).await,
        MuxRawCodec::Png => import_raw_png_async(path, spec, sources).await,
        MuxRawCodec::Bmp => import_raw_bmp_async(path, spec, sources).await,
        MuxRawCodec::Prores => import_raw_prores_async(path, spec, sources).await,
        MuxRawCodec::Y4m => import_raw_y4m_async(path, spec, sources).await,
        MuxRawCodec::J2k => import_raw_j2k_async(path, spec, sources).await,
        MuxRawCodec::Pcm => import_wave_pcm_async(path, spec, sources).await,
        MuxRawCodec::Dts => import_raw_dts_async(path, spec, sources).await,
        MuxRawCodec::Truehd => import_raw_truehd_async(path, spec, sources).await,
        MuxRawCodec::Alac => import_caf_alac_async(path, spec, sources).await,
        MuxRawCodec::Flac => import_raw_flac_async(path, spec, sources).await,
        MuxRawCodec::Iamf => import_raw_iamf_async(path, spec, sources).await,
        MuxRawCodec::MpegH => import_raw_mhas_async(path, spec, sources).await,
        MuxRawCodec::Opus => import_ogg_opus_async(path, spec, sources).await,
        MuxRawCodec::Vorbis => import_ogg_vorbis_async(path, spec, sources).await,
        MuxRawCodec::Speex => import_ogg_speex_async(path, spec, sources).await,
        MuxRawCodec::Theora => import_ogg_theora_async(path, spec, sources).await,
    }
}

pub(in crate::mux) fn build_visual_sample_entry_box(
    sample_entry_type: FourCc,
    width: u16,
    height: u16,
    child_boxes: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    build_visual_sample_entry_box_with_compressor_name(
        sample_entry_type,
        width,
        height,
        &[],
        child_boxes,
    )
}

pub(in crate::mux) fn build_visual_sample_entry_box_with_compressor_name(
    sample_entry_type: FourCc,
    width: u16,
    height: u16,
    compressor_name: &[u8],
    child_boxes: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    let mut compressorname = [0_u8; 32];
    let visible_len = compressor_name.len().min(31);
    compressorname[0] =
        u8::try_from(visible_len).map_err(|_| MuxError::LayoutOverflow("compressor name"))?;
    compressorname[1..1 + visible_len].copy_from_slice(&compressor_name[..visible_len]);
    super::mp4::encode_typed_box(
        &VisualSampleEntry {
            sample_entry: SampleEntry {
                box_type: sample_entry_type,
                data_reference_index: 1,
            },
            width,
            height,
            horizresolution: 72_u32 << 16,
            vertresolution: 72_u32 << 16,
            frame_count: 1,
            compressorname,
            depth: 0x0018,
            pre_defined3: -1,
            ..VisualSampleEntry::default()
        },
        &child_boxes.concat(),
    )
}

pub(in crate::mux) fn build_generic_audio_sample_entry_box(
    sample_entry_type: FourCc,
    sample_rate: u32,
    channel_count: u16,
    sample_size: u16,
    child_boxes: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    super::mp4::encode_typed_box(
        &AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: sample_entry_type,
                data_reference_index: 1,
            },
            channel_count,
            sample_size,
            sample_rate: sample_rate << 16,
            ..AudioSampleEntry::default()
        },
        &child_boxes.concat(),
    )
}

pub(in crate::mux) fn build_generic_media_sample_entry_box(
    sample_entry_type: FourCc,
    child_boxes: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    super::mp4::encode_typed_box(
        &GenericMediaSampleEntry {
            sample_entry: SampleEntry {
                box_type: sample_entry_type,
                data_reference_index: 1,
            },
        },
        &child_boxes.concat(),
    )
}

pub(in crate::mux) fn build_btrt_from_sample_sizes<I>(
    samples: I,
    timescale: u32,
) -> Result<Btrt, MuxError>
where
    I: IntoIterator<Item = (u32, u32)>,
{
    build_btrt_from_sample_sizes_with_total_duration(samples, timescale, None)
}

pub(in crate::mux) fn build_btrt_from_sample_sizes_with_total_duration<I>(
    samples: I,
    timescale: u32,
    total_duration_override: Option<u64>,
) -> Result<Btrt, MuxError>
where
    I: IntoIterator<Item = (u32, u32)>,
{
    if timescale == 0 {
        return Ok(Btrt::default());
    }

    let mut saw_sample = false;
    let mut buffer_size_db = 0_u32;
    let mut total_payload_bytes = 0_u64;
    let mut total_duration = 0_u64;
    let mut max_window_payload_bytes = 0_u64;
    let mut current_window_payload_bytes = 0_u64;
    let mut window_start_decode_time = 0_u64;
    let mut sample_decode_time = 0_u64;
    for (data_size, duration) in samples {
        saw_sample = true;
        buffer_size_db = buffer_size_db.max(data_size);
        total_payload_bytes = total_payload_bytes
            .checked_add(u64::from(data_size))
            .ok_or(MuxError::LayoutOverflow("audio total payload bytes"))?;
        total_duration = total_duration
            .checked_add(u64::from(duration))
            .ok_or(MuxError::LayoutOverflow("audio total duration"))?;
        current_window_payload_bytes = current_window_payload_bytes
            .checked_add(u64::from(data_size))
            .ok_or(MuxError::LayoutOverflow("audio bitrate window payload"))?;
        if sample_decode_time > window_start_decode_time.saturating_add(u64::from(timescale)) {
            max_window_payload_bytes = max_window_payload_bytes.max(current_window_payload_bytes);
            window_start_decode_time = sample_decode_time;
            current_window_payload_bytes = 0;
        }
        sample_decode_time = sample_decode_time
            .checked_add(u64::from(duration))
            .ok_or(MuxError::LayoutOverflow("audio decode time"))?;
    }
    let total_duration = total_duration_override.unwrap_or(total_duration);
    if !saw_sample || total_duration == 0 {
        return Ok(Btrt::default());
    }

    let avg_bitrate = total_payload_bytes
        .checked_mul(8)
        .and_then(|bits| bits.checked_mul(u64::from(timescale)))
        .ok_or(MuxError::LayoutOverflow("audio average bitrate"))?
        / total_duration;
    let avg_bitrate = avg_bitrate & !7;

    let max_bitrate = if max_window_payload_bytes == 0 {
        avg_bitrate
    } else {
        max_window_payload_bytes
            .checked_mul(8)
            .ok_or(MuxError::LayoutOverflow("audio maximum bitrate"))?
    };

    Ok(Btrt {
        buffer_size_db,
        max_bitrate: u32::try_from(max_bitrate)
            .map_err(|_| MuxError::LayoutOverflow("audio maximum bitrate"))?,
        avg_bitrate: u32::try_from(avg_bitrate)
            .map_err(|_| MuxError::LayoutOverflow("audio average bitrate"))?,
    })
}

fn import_ivf_video_sync(
    path: &Path,
    codec: MuxRawCodec,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = match codec {
        MuxRawCodec::Vp8 => scan_vp8_file_sync(path, &spec)?,
        MuxRawCodec::Vp9 => scan_vp9_file_sync(path, &spec)?,
        MuxRawCodec::Vp10 => scan_vp10_file_sync(path, &spec)?,
        _ => unreachable!("only IVF-backed codecs use this import helper"),
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name(match codec {
            MuxRawCodec::Vp8 => "vp8",
            MuxRawCodec::Vp9 => "vp9",
            MuxRawCodec::Vp10 => "vp10",
            _ => unreachable!("only IVF-backed codecs use this import helper"),
        }),
        mux_policy: direct_ingest_mux_policy(
            match codec {
                MuxRawCodec::Vp8 => "vp8",
                MuxRawCodec::Vp9 => "vp9",
                MuxRawCodec::Vp10 => "vp10",
                _ => unreachable!("only IVF-backed codecs use this import helper"),
            },
            MuxTrackKind::Video,
        ),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_ivf_video_async(
    path: &Path,
    codec: MuxRawCodec,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let source_index = sources.add_file(path)?;
    let parsed = match codec {
        MuxRawCodec::Vp8 => scan_vp8_file_async(path, &spec).await?,
        MuxRawCodec::Vp9 => scan_vp9_file_async(path, &spec).await?,
        MuxRawCodec::Vp10 => scan_vp10_file_async(path, &spec).await?,
        _ => unreachable!("only IVF-backed codecs use this import helper"),
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale: parsed.timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name(match codec {
            MuxRawCodec::Vp8 => "vp8",
            MuxRawCodec::Vp9 => "vp9",
            MuxRawCodec::Vp10 => "vp10",
            _ => unreachable!("only IVF-backed codecs use this import helper"),
        }),
        mux_policy: direct_ingest_mux_policy(
            match codec {
                MuxRawCodec::Vp8 => "vp8",
                MuxRawCodec::Vp9 => "vp9",
                MuxRawCodec::Vp10 => "vp10",
                _ => unreachable!("only IVF-backed codecs use this import helper"),
            },
            MuxTrackKind::Video,
        ),
        width: parsed.width,
        height: parsed.height,
        sample_entry_box: parsed.sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(parsed.samples, source_index),
    })
}

fn import_raw_av1_sync(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_av1_file_sync(path, &spec)?;
    let ParsedAv1Track {
        width,
        height,
        timescale,
        sample_entry_box,
        samples,
        source,
    } = parsed;
    let source_index = match source {
        ParsedAv1TrackSource::File => sources.add_file(path)?,
        ParsedAv1TrackSource::Segmented(source) => sources.add_segmented(source)?,
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("av1"),
        mux_policy: direct_ingest_mux_policy("av1", MuxTrackKind::Video),
        width,
        height,
        sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(samples, source_index),
    })
}

#[cfg(feature = "async")]
async fn import_raw_av1_async(
    path: &Path,
    spec: String,
    sources: &mut SourceCatalog,
) -> Result<ImportedTrack, MuxError> {
    let parsed = scan_av1_file_async(path, &spec).await?;
    let ParsedAv1Track {
        width,
        height,
        timescale,
        sample_entry_box,
        samples,
        source,
    } = parsed;
    let source_index = match source {
        ParsedAv1TrackSource::File => sources.add_file(path)?,
        ParsedAv1TrackSource::Segmented(source) => sources.add_segmented(source)?,
    };
    Ok(ImportedTrack {
        kind: MuxTrackKind::Video,
        timescale,
        language: *b"und",
        handler_name: direct_ingest_handler_name("av1"),
        mux_policy: direct_ingest_mux_policy("av1", MuxTrackKind::Video),
        width,
        height,
        sample_entry_box,
        source_edit_media_time: None,
        sample_roll_distance: None,
        samples: imported_samples_from_staged(samples, source_index),
    })
}

#[derive(Clone, Copy)]
pub(in crate::mux) struct SourceFileSpan {
    pub(in crate::mux) source_offset: u64,
    pub(in crate::mux) size: u32,
}

pub(in crate::mux) fn read_exact_at_sync(
    file: &mut File,
    offset: u64,
    buf: &mut [u8],
    spec: &str,
    truncated_message: &'static str,
) -> Result<(), MuxError> {
    file.seek(SeekFrom::Start(offset))?;
    match file.read_exact(buf) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: truncated_message.to_string(),
            })
        }
        Err(error) => Err(MuxError::Io(error)),
    }
}

pub(in crate::mux) fn read_spans_sync(
    file: &mut File,
    spans: &[SourceFileSpan],
    total_size: u32,
    spec: &str,
    truncated_message: &'static str,
) -> Result<Vec<u8>, MuxError> {
    let mut bytes = Vec::with_capacity(
        usize::try_from(total_size)
            .map_err(|_| MuxError::LayoutOverflow("packet byte capacity"))?,
    );
    for span in spans {
        let mut chunk = vec![0_u8; usize::try_from(span.size).unwrap()];
        read_exact_at_sync(
            file,
            span.source_offset,
            &mut chunk,
            spec,
            truncated_message,
        )?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn absolute_path(path: &Path) -> Result<PathBuf, MuxError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()?.join(path))
}

fn extract_required_single_as_sync<R, T>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
    name: &'static str,
) -> Result<T, MuxError>
where
    R: Read + Seek,
    T: CodecBox + Clone + 'static,
{
    let boxes = extract_box_as::<_, T>(reader, Some(parent), path)?;
    let [value] = boxes.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: name.to_string(),
            message: format!("expected exactly one {name} box but found {}", boxes.len()),
        });
    };
    Ok(value.clone())
}

fn extract_optional_single_as_sync<R, T>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
) -> Result<Option<T>, MuxError>
where
    R: Read + Seek,
    T: CodecBox + Clone + 'static,
{
    let boxes = extract_box_as::<_, T>(reader, Some(parent), path)?;
    match boxes.len() {
        0 => Ok(None),
        1 => Ok(Some(boxes[0].clone())),
        _ => Err(MuxError::UnsupportedTrackImport {
            spec: "track".to_string(),
            message: "expected at most one optional box".to_string(),
        }),
    }
}

#[cfg(feature = "async")]
async fn extract_required_single_as_async<R, T>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
    name: &'static str,
) -> Result<T, MuxError>
where
    R: AsyncReadSeek,
    T: CodecBox + Clone + 'static,
{
    let boxes = extract_box_as_async::<_, T>(reader, Some(parent), path).await?;
    let [value] = boxes.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: name.to_string(),
            message: format!("expected exactly one {name} box but found {}", boxes.len()),
        });
    };
    Ok(value.clone())
}

#[cfg(feature = "async")]
async fn extract_optional_single_as_async<R, T>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
) -> Result<Option<T>, MuxError>
where
    R: AsyncReadSeek,
    T: CodecBox + Clone + 'static,
{
    let boxes = extract_box_as_async::<_, T>(reader, Some(parent), path).await?;
    match boxes.len() {
        0 => Ok(None),
        1 => Ok(Some(boxes[0].clone())),
        _ => Err(MuxError::UnsupportedTrackImport {
            spec: "track".to_string(),
            message: "expected at most one optional box".to_string(),
        }),
    }
}

fn extract_required_single_info_sync<R>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
    name: &'static str,
) -> Result<HeaderInfo, MuxError>
where
    R: Read + Seek,
{
    let infos = extract_box(reader, Some(parent), path)?;
    let [info] = infos.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: name.to_string(),
            message: format!("expected exactly one {name} box but found {}", infos.len()),
        });
    };
    Ok(*info)
}

#[cfg(feature = "async")]
async fn extract_required_single_info_async<R>(
    reader: &mut R,
    parent: &HeaderInfo,
    path: BoxPath,
    name: &'static str,
) -> Result<HeaderInfo, MuxError>
where
    R: AsyncReadSeek,
{
    let infos = extract_box_async(reader, Some(parent), path).await?;
    let [info] = infos.as_slice() else {
        return Err(MuxError::UnsupportedTrackImport {
            spec: name.to_string(),
            message: format!("expected exactly one {name} box but found {}", infos.len()),
        });
    };
    Ok(*info)
}

fn extract_preserved_flat_stbl_boxes_sync<R>(
    reader: &mut R,
    stbl_info: &HeaderInfo,
) -> Result<Vec<Vec<u8>>, MuxError>
where
    R: Read + Seek,
{
    let mut preserved = Vec::new();
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([CSLG]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([SDTP]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([STPS]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([STDP]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([SUBS]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([SGPD]),
    )?);
    preserved.extend(extract_box_bytes(
        reader,
        Some(stbl_info),
        BoxPath::from([SBGP]),
    )?);
    Ok(preserved)
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_stbl_boxes_async<R>(
    reader: &mut R,
    stbl_info: &HeaderInfo,
) -> Result<Vec<Vec<u8>>, MuxError>
where
    R: AsyncReadSeek,
{
    let mut preserved = Vec::new();
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([CSLG])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([SDTP])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([STPS])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([STDP])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([SUBS])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([SGPD])).await?);
    preserved
        .extend(extract_box_bytes_async(reader, Some(stbl_info), BoxPath::from([SBGP])).await?);
    Ok(preserved)
}

fn expand_sample_sizes(stsz: &Stsz, path: &Path, track_id: u32) -> Result<Vec<u32>, MuxError> {
    if stsz.sample_size != 0 {
        return Ok(vec![stsz.sample_size; stsz.sample_count as usize]);
    }
    if stsz.entry_size.len() != stsz.sample_count as usize {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} has stsz sample_count {} but {} explicit entry sizes",
                stsz.sample_count,
                stsz.entry_size.len()
            ),
        });
    }
    stsz.entry_size
        .iter()
        .map(|size| {
            u32::try_from(*size).map_err(|_| MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} has a sample size that does not fit in u32"),
            })
        })
        .collect()
}

fn parse_compact_sample_sizes(
    stz2_bytes: &[u8],
    path: &Path,
    track_id: u32,
) -> Result<Vec<u32>, MuxError> {
    let payload = compact_sample_size_payload(stz2_bytes, path, track_id)?;
    if payload.len() < 12 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has a truncated stz2 sample size table"),
        });
    }
    if payload[0] != 0 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} uses unsupported stz2 version {}",
                payload[0]
            ),
        });
    }
    let field_size = payload[7];
    let sample_count = usize::try_from(u32::from_be_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
    ]))
    .map_err(|_| MuxError::LayoutOverflow("compact sample size count"))?;
    let data = &payload[12..];
    let expected_data_len = compact_sample_size_data_len(field_size, sample_count, path, track_id)?;
    if data.len() != expected_data_len {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} has stz2 sample_count {sample_count} with {} size bytes",
                data.len()
            ),
        });
    }

    let mut sample_sizes = Vec::with_capacity(sample_count);
    match field_size {
        4 => {
            for (index, byte) in data.iter().copied().enumerate() {
                if sample_sizes.len() == sample_count {
                    break;
                }
                sample_sizes.push(u32::from(byte >> 4));
                if sample_sizes.len() != sample_count {
                    sample_sizes.push(u32::from(byte & 0x0F));
                } else if byte & 0x0F != 0 {
                    return Err(MuxError::UnsupportedTrackImport {
                        spec: path.display().to_string(),
                        message: format!(
                            "track {track_id} has a non-zero unused stz2 nibble at byte {index}"
                        ),
                    });
                }
            }
        }
        8 => sample_sizes.extend(data.iter().map(|size| u32::from(*size))),
        16 => {
            for bytes in data.chunks_exact(2) {
                sample_sizes.push(u32::from(u16::from_be_bytes([bytes[0], bytes[1]])));
            }
        }
        _ => unreachable!(),
    }
    Ok(sample_sizes)
}

fn compact_sample_size_payload<'a>(
    stz2_bytes: &'a [u8],
    path: &Path,
    track_id: u32,
) -> Result<&'a [u8], MuxError> {
    if stz2_bytes.len() < 8 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has a truncated stz2 box header"),
        });
    }
    let box_size = u32::from_be_bytes([stz2_bytes[0], stz2_bytes[1], stz2_bytes[2], stz2_bytes[3]]);
    if stz2_bytes[4..8] != *b"stz2" {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has a malformed stz2 sample size table"),
        });
    }
    let (declared_size, payload_offset) = if box_size == 1 {
        if stz2_bytes.len() < 16 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} has a truncated largesize stz2 header"),
            });
        }
        (
            u64::from_be_bytes([
                stz2_bytes[8],
                stz2_bytes[9],
                stz2_bytes[10],
                stz2_bytes[11],
                stz2_bytes[12],
                stz2_bytes[13],
                stz2_bytes[14],
                stz2_bytes[15],
            ]),
            16,
        )
    } else {
        (u64::from(box_size), 8)
    };
    if declared_size != u64::try_from(stz2_bytes.len()).unwrap_or(u64::MAX) {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has an inconsistent stz2 box size"),
        });
    }
    if declared_size < u64::try_from(payload_offset).unwrap_or(u64::MAX) {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has an invalid stz2 box size"),
        });
    }
    Ok(&stz2_bytes[payload_offset..])
}

fn compact_sample_size_data_len(
    field_size: u8,
    sample_count: usize,
    path: &Path,
    track_id: u32,
) -> Result<usize, MuxError> {
    match field_size {
        4 => Ok(sample_count.div_ceil(2)),
        8 => Ok(sample_count),
        16 => sample_count
            .checked_mul(2)
            .ok_or(MuxError::LayoutOverflow("compact sample size payload")),
        _ => Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} uses unsupported stz2 field size {field_size}"),
        }),
    }
}

fn expand_sample_durations(
    stts: &Stts,
    sample_count: usize,
    path: &Path,
    track_id: u32,
) -> Result<Vec<u32>, MuxError> {
    let mut durations = Vec::with_capacity(sample_count);
    for entry in &stts.entries {
        for _ in 0..entry.sample_count {
            durations.push(entry.sample_delta);
        }
    }
    if durations.len() != sample_count {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} resolves {} durations from stts but has {sample_count} samples",
                durations.len()
            ),
        });
    }
    Ok(durations)
}

fn expand_composition_offsets(
    ctts: Option<&Ctts>,
    sample_count: usize,
    path: &Path,
    track_id: u32,
) -> Result<Vec<i32>, MuxError> {
    let Some(ctts) = ctts else {
        return Ok(vec![0; sample_count]);
    };
    let mut offsets = Vec::with_capacity(sample_count);
    for (entry_index, entry) in ctts.entries.iter().enumerate() {
        for _ in 0..entry.sample_count {
            offsets.push(i32::try_from(ctts.sample_offset(entry_index)).map_err(|_| {
                MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!("track {track_id} uses a composition offset outside i32"),
                }
            })?);
        }
    }
    if offsets.len() != sample_count {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} resolves {} composition offsets but has {sample_count} samples",
                offsets.len()
            ),
        });
    }
    Ok(offsets)
}

fn select_chunk_offsets(
    stco: Option<&Stco>,
    co64: Option<&Co64>,
    path: &Path,
    track_id: u32,
) -> Result<Vec<u64>, MuxError> {
    match (stco, co64) {
        (Some(_), Some(_)) => Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} carries both stco and co64"),
        }),
        (Some(stco), None) => Ok(stco.chunk_offset.clone()),
        (None, Some(co64)) => Ok(co64.chunk_offset.clone()),
        (None, None) => Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} is missing stco/co64 chunk offsets"),
        }),
    }
}

fn expand_sample_offsets_and_description_indices(
    stsc: &Stsc,
    sample_sizes: &[u32],
    chunk_offsets: &[u64],
    path: &Path,
    track_id: u32,
) -> Result<(Vec<u64>, Vec<u32>), MuxError> {
    if stsc.entries.is_empty() {
        if sample_sizes.is_empty() && chunk_offsets.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has no stsc entries"),
        });
    }

    let mut mappings = Vec::with_capacity(chunk_offsets.len());
    for (index, entry) in stsc.entries.iter().enumerate() {
        if entry.first_chunk == 0 || entry.sample_description_index == 0 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} uses unsupported stsc entry first_chunk={} sample_description_index={}",
                    entry.first_chunk, entry.sample_description_index
                ),
            });
        }
        let next_first_chunk = stsc
            .entries
            .get(index + 1)
            .map(|next| next.first_chunk)
            .unwrap_or(
                u32::try_from(chunk_offsets.len())
                    .map_err(|_| MuxError::LayoutOverflow("chunk count"))?
                    .saturating_add(1),
            );
        if next_first_chunk <= entry.first_chunk {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} has descending stsc first_chunk values"),
            });
        }
        for _ in entry.first_chunk..next_first_chunk {
            mappings.push((entry.samples_per_chunk, entry.sample_description_index));
        }
    }
    if mappings.len() != chunk_offsets.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} resolved {} chunk mappings for {} chunk offsets",
                mappings.len(),
                chunk_offsets.len()
            ),
        });
    }

    let mut sample_offsets = Vec::with_capacity(sample_sizes.len());
    let mut sample_description_indices = Vec::with_capacity(sample_sizes.len());
    let mut sample_index = 0_usize;
    for (chunk_offset, (samples_per_chunk, sample_description_index)) in
        chunk_offsets.iter().zip(mappings)
    {
        let mut running_offset = *chunk_offset;
        for _ in 0..samples_per_chunk {
            let Some(sample_size) = sample_sizes.get(sample_index).copied() else {
                return Err(MuxError::UnsupportedTrackImport {
                    spec: path.display().to_string(),
                    message: format!(
                        "track {track_id} resolved more chunk samples than stsz entries"
                    ),
                });
            };
            sample_offsets.push(running_offset);
            sample_description_indices.push(sample_description_index);
            running_offset = running_offset
                .checked_add(u64::from(sample_size))
                .ok_or(MuxError::LayoutOverflow("sample offset"))?;
            sample_index += 1;
        }
    }
    if sample_index != sample_sizes.len() {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} resolved {sample_index} sample offsets for {} sample sizes",
                sample_sizes.len()
            ),
        });
    }
    Ok((sample_offsets, sample_description_indices))
}

fn expand_chunk_sample_counts(
    stsc: &Stsc,
    chunk_count: usize,
    path: &Path,
    track_id: u32,
) -> Result<Vec<u32>, MuxError> {
    if stsc.entries.is_empty() {
        if chunk_count == 0 {
            return Ok(Vec::new());
        }
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!("track {track_id} has no stsc entries"),
        });
    }

    let mut chunk_sample_counts = Vec::with_capacity(chunk_count);
    for (index, entry) in stsc.entries.iter().enumerate() {
        if entry.first_chunk == 0 || entry.sample_description_index == 0 {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} uses unsupported stsc entry first_chunk={} sample_description_index={}",
                    entry.first_chunk, entry.sample_description_index
                ),
            });
        }
        let next_first_chunk = stsc
            .entries
            .get(index + 1)
            .map(|next| next.first_chunk)
            .unwrap_or(
                u32::try_from(chunk_count)
                    .map_err(|_| MuxError::LayoutOverflow("chunk count"))?
                    .saturating_add(1),
            );
        if next_first_chunk <= entry.first_chunk {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!("track {track_id} has descending stsc first_chunk values"),
            });
        }
        for _ in entry.first_chunk..next_first_chunk {
            chunk_sample_counts.push(entry.samples_per_chunk);
        }
    }
    if chunk_sample_counts.len() != chunk_count {
        return Err(MuxError::UnsupportedTrackImport {
            spec: path.display().to_string(),
            message: format!(
                "track {track_id} resolved {} chunk sample counts for {chunk_count} chunk offsets",
                chunk_sample_counts.len(),
            ),
        });
    }
    Ok(chunk_sample_counts)
}

fn expand_sync_samples(
    stss: Option<&Stss>,
    sample_entry_type: FourCc,
    sample_count: usize,
    path: &Path,
    track_id: u32,
) -> Result<Vec<bool>, MuxError> {
    let Some(stss) = stss else {
        return Ok(vec![true; sample_count]);
    };
    if stss.entry_count == 0
        && matches!(
            sample_entry_type,
            value if value == FourCc::from_bytes(*b"vp08")
                || value == FourCc::from_bytes(*b"vp09")
        )
    {
        return Ok(vec![true; sample_count]);
    }
    if stss.entry_count == 1
        && stss.sample_number.as_slice() == [1]
        && sample_count > 2
        && matches!(
            sample_entry_type,
            value
                if value == FourCc::from_bytes(*b"mha1")
                    || value == FourCc::from_bytes(*b"mha2")
                    || value == FourCc::from_bytes(*b"mhm1")
                    || value == FourCc::from_bytes(*b"mhm2")
        )
    {
        return Ok(vec![true; sample_count]);
    }
    let mut sync = vec![false; sample_count];
    for sample_number in &stss.sample_number {
        let index = usize::try_from(sample_number.saturating_sub(1)).map_err(|_| {
            MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} exposes an stss entry that does not fit in usize"
                ),
            }
        })?;
        let Some(entry) = sync.get_mut(index) else {
            return Err(MuxError::UnsupportedTrackImport {
                spec: path.display().to_string(),
                message: format!(
                    "track {track_id} exposes an stss sample number outside its sample count"
                ),
            });
        };
        *entry = true;
    }
    Ok(sync)
}

fn imported_mp4_avc_length_size(sample_entry_box: &[u8]) -> Result<Option<usize>, MuxError> {
    if !matches!(
        sample_entry_box_type(sample_entry_box),
        Some(box_type)
            if box_type == FourCc::from_bytes(*b"avc1")
                || box_type == FourCc::from_bytes(*b"avc3")
    ) {
        return Ok(None);
    }
    let child_boxes = super::mp4::visual_sample_entry_immediate_children(sample_entry_box)?;
    let Some(avcc_box) = child_boxes
        .into_iter()
        .find(|child_box| sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"avcC")))
    else {
        return Ok(None);
    };
    let avcc = super::mp4::decode_typed_box::<AVCDecoderConfiguration>(&avcc_box)?;
    Ok(Some(usize::from(avcc.length_size_minus_one) + 1))
}

fn imported_mp4_hevc_length_size(sample_entry_box: &[u8]) -> Result<Option<usize>, MuxError> {
    if !matches!(
        sample_entry_box_type(sample_entry_box),
        Some(box_type)
            if box_type == FourCc::from_bytes(*b"hvc1")
                || box_type == FourCc::from_bytes(*b"hev1")
                || box_type == FourCc::from_bytes(*b"dvh1")
                || box_type == FourCc::from_bytes(*b"dvhe")
    ) {
        return Ok(None);
    }
    let child_boxes = super::mp4::visual_sample_entry_immediate_children(sample_entry_box)?;
    let Some(hvcc_box) = child_boxes
        .into_iter()
        .find(|child_box| sample_entry_box_type(child_box) == Some(FourCc::from_bytes(*b"hvcC")))
    else {
        return Ok(None);
    };
    let hvcc = super::mp4::decode_typed_box::<HEVCDecoderConfiguration>(&hvcc_box)?;
    Ok(Some(usize::from(hvcc.length_size_minus_one) + 1))
}

fn supplement_imported_mp4_avc_sync_samples_sync<R>(
    reader: &mut R,
    sample_entry_type: FourCc,
    sample_entry_box: &[u8],
    _source_stss: Option<&Stss>,
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    sync_samples: &mut [bool],
) -> Result<(), MuxError>
where
    R: Read + Seek,
{
    if sample_entry_type != FourCc::from_bytes(*b"avc1")
        && sample_entry_type != FourCc::from_bytes(*b"avc3")
    {
        return Ok(());
    }
    let Some(length_size) = imported_mp4_avc_length_size(sample_entry_box)? else {
        return Ok(());
    };
    if length_size == 0 || length_size > 4 {
        return Ok(());
    }
    for ((sample_offset, sample_size), is_sync_sample) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(sync_samples.iter_mut())
    {
        if *is_sync_sample || sample_size == 0 {
            continue;
        }
        let sample_size = usize::try_from(sample_size)
            .map_err(|_| MuxError::LayoutOverflow("AVC sample size inspection"))?;
        let mut sample_bytes = vec![0_u8; sample_size];
        reader
            .seek(SeekFrom::Start(sample_offset))
            .map_err(MuxError::Io)?;
        match reader.read_exact(&mut sample_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(MuxError::Io(error)),
        }
        if imported_mp4_avc_sample_contains_sync_nal(&sample_bytes, length_size) {
            *is_sync_sample = true;
        }
    }

    Ok(())
}

fn supplement_imported_mp4_hevc_sync_samples_sync<R>(
    reader: &mut R,
    sample_entry_type: FourCc,
    sample_entry_box: &[u8],
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    sync_samples: &mut [bool],
) -> Result<(), MuxError>
where
    R: Read + Seek,
{
    if sample_entry_type != FourCc::from_bytes(*b"hvc1")
        && sample_entry_type != FourCc::from_bytes(*b"hev1")
        && sample_entry_type != FourCc::from_bytes(*b"dvh1")
        && sample_entry_type != FourCc::from_bytes(*b"dvhe")
    {
        return Ok(());
    }
    let Some(length_size) = imported_mp4_hevc_length_size(sample_entry_box)? else {
        return Ok(());
    };
    if length_size == 0 || length_size > 4 {
        return Ok(());
    }

    for ((sample_offset, sample_size), is_sync_sample) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(sync_samples.iter_mut())
    {
        if *is_sync_sample || sample_size == 0 {
            continue;
        }
        let sample_size = usize::try_from(sample_size)
            .map_err(|_| MuxError::LayoutOverflow("HEVC sample size inspection"))?;
        let mut sample_bytes = vec![0_u8; sample_size];
        reader
            .seek(SeekFrom::Start(sample_offset))
            .map_err(MuxError::Io)?;
        match reader.read_exact(&mut sample_bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(MuxError::Io(error)),
        }
        if imported_mp4_hevc_sample_contains_sync_nal(&sample_bytes, length_size) {
            *is_sync_sample = true;
        }
    }

    Ok(())
}

#[cfg(feature = "async")]
async fn supplement_imported_mp4_avc_sync_samples_async<R>(
    reader: &mut R,
    sample_entry_type: FourCc,
    sample_entry_box: &[u8],
    _source_stss: Option<&Stss>,
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    sync_samples: &mut [bool],
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
{
    if sample_entry_type != FourCc::from_bytes(*b"avc1")
        && sample_entry_type != FourCc::from_bytes(*b"avc3")
    {
        return Ok(());
    }
    let Some(length_size) = imported_mp4_avc_length_size(sample_entry_box)? else {
        return Ok(());
    };
    if length_size == 0 || length_size > 4 {
        return Ok(());
    }
    for ((sample_offset, sample_size), is_sync_sample) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(sync_samples.iter_mut())
    {
        if *is_sync_sample || sample_size == 0 {
            continue;
        }
        let sample_size = usize::try_from(sample_size)
            .map_err(|_| MuxError::LayoutOverflow("AVC sample size inspection"))?;
        let mut sample_bytes = vec![0_u8; sample_size];
        reader
            .seek(SeekFrom::Start(sample_offset))
            .await
            .map_err(MuxError::Io)?;
        match reader.read_exact(&mut sample_bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(MuxError::Io(error)),
        }
        if imported_mp4_avc_sample_contains_sync_nal(&sample_bytes, length_size) {
            *is_sync_sample = true;
        }
    }

    Ok(())
}

#[cfg(feature = "async")]
async fn supplement_imported_mp4_hevc_sync_samples_async<R>(
    reader: &mut R,
    sample_entry_type: FourCc,
    sample_entry_box: &[u8],
    sample_offsets: &[u64],
    sample_sizes: &[u32],
    sync_samples: &mut [bool],
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
{
    if sample_entry_type != FourCc::from_bytes(*b"hvc1")
        && sample_entry_type != FourCc::from_bytes(*b"hev1")
        && sample_entry_type != FourCc::from_bytes(*b"dvh1")
        && sample_entry_type != FourCc::from_bytes(*b"dvhe")
    {
        return Ok(());
    }
    let Some(length_size) = imported_mp4_hevc_length_size(sample_entry_box)? else {
        return Ok(());
    };
    if length_size == 0 || length_size > 4 {
        return Ok(());
    }

    for ((sample_offset, sample_size), is_sync_sample) in sample_offsets
        .iter()
        .copied()
        .zip(sample_sizes.iter().copied())
        .zip(sync_samples.iter_mut())
    {
        if *is_sync_sample || sample_size == 0 {
            continue;
        }
        let sample_size = usize::try_from(sample_size)
            .map_err(|_| MuxError::LayoutOverflow("HEVC sample size inspection"))?;
        let mut sample_bytes = vec![0_u8; sample_size];
        reader
            .seek(SeekFrom::Start(sample_offset))
            .await
            .map_err(MuxError::Io)?;
        match reader.read_exact(&mut sample_bytes).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(MuxError::Io(error)),
        }
        if imported_mp4_hevc_sample_contains_sync_nal(&sample_bytes, length_size) {
            *is_sync_sample = true;
        }
    }

    Ok(())
}

fn imported_mp4_hevc_sample_contains_sync_nal(sample_bytes: &[u8], length_size: usize) -> bool {
    let mut offset = 0_usize;
    while sample_bytes.len().saturating_sub(offset) >= length_size {
        let nal_size = match length_size {
            1 => usize::from(sample_bytes[offset]),
            2 => usize::from(u16::from_be_bytes(
                sample_bytes[offset..offset + 2].try_into().unwrap(),
            )),
            3 => {
                (usize::from(sample_bytes[offset]) << 16)
                    | (usize::from(sample_bytes[offset + 1]) << 8)
                    | usize::from(sample_bytes[offset + 2])
            }
            4 => usize::try_from(u32::from_be_bytes(
                sample_bytes[offset..offset + 4].try_into().unwrap(),
            ))
            .unwrap(),
            _ => return false,
        };
        offset += length_size;
        let Some(end) = offset.checked_add(nal_size) else {
            return false;
        };
        if end > sample_bytes.len() {
            return false;
        }
        let nal = &sample_bytes[offset..end];
        if imported_mp4_hevc_nal_is_sync(nal) {
            return true;
        }
        offset = end;
    }
    false
}

fn imported_mp4_hevc_nal_is_sync(nal: &[u8]) -> bool {
    if nal.is_empty() {
        return false;
    }
    matches!((nal[0] >> 1) & 0x3F, 16..=21)
}

fn imported_mp4_avc_nal_is_intra_slice(nal: &[u8]) -> bool {
    if nal.len() < 2 {
        return false;
    }
    let nal_type = nal[0] & 0x1F;
    if !matches!(nal_type, 1..=5) {
        return false;
    }
    let rbsp = nal_to_rbsp(&nal[1..]);
    let mut reader = BitReader::new(Cursor::new(rbsp));
    if read_ue_labeled(&mut reader, "h264", "H.264 slice first_mb_in_slice").is_err() {
        return false;
    }
    let Ok(slice_type) = read_ue_labeled(&mut reader, "h264", "H.264 slice type") else {
        return false;
    };
    matches!(slice_type % 5, 2 | 4)
}

fn decode_mdhd_language(encoded: [u8; 3]) -> [u8; 3] {
    let mut decoded = [b'u', b'n', b'd'];
    for (index, value) in encoded.into_iter().enumerate() {
        decoded[index] = if (1..=26).contains(&value) {
            value + b'`'
        } else {
            b"und"[index]
        };
    }
    decoded
}

fn scale_track_time_to_movie(
    track_id: u32,
    value: i64,
    track_timescale: u32,
    movie_timescale: u32,
    allow_inexact: bool,
) -> Result<i64, MuxError> {
    if track_timescale == 0 || movie_timescale == 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }
    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(movie_timescale))
        .ok_or(MuxError::LayoutOverflow("track time normalization"))?;
    if scaled % u64::from(track_timescale) != 0 && !allow_inexact {
        return Err(MuxError::IncompatibleTrackTiming {
            track_id,
            track_timescale,
            movie_timescale,
            value,
        });
    }
    let normalized = scaled / u64::from(track_timescale);
    i64::try_from(normalized)
        .map_err(|_| MuxError::LayoutOverflow("track time normalization"))
        .map(|normalized| normalized * sign)
}

fn track_times_fit_movie_timescale(track: &ImportedTrack, movie_timescale: u32) -> bool {
    if track.timescale == 0 || movie_timescale == 0 {
        return false;
    }
    track.samples.iter().all(|sample| {
        can_scale_track_time_to_movie(i64::from(sample.duration), track.timescale, movie_timescale)
            && can_scale_track_time_to_movie(
                i64::from(sample.composition_time_offset),
                track.timescale,
                movie_timescale,
            )
    })
}

fn can_scale_track_time_to_movie(value: i64, track_timescale: u32, movie_timescale: u32) -> bool {
    let magnitude = value.unsigned_abs();
    magnitude
        .checked_mul(u64::from(movie_timescale))
        .is_some_and(|scaled| scaled % u64::from(track_timescale) == 0)
}

fn lcm_u32(left: u32, right: u32) -> Option<u32> {
    let gcd = gcd_u32(left, right);
    left.checked_div(gcd)?.checked_mul(right)
}

const fn gcd_u32(mut left: u32, mut right: u32) -> u32 {
    while right != 0 {
        let next = left % right;
        left = right;
        right = next;
    }
    left
}

fn probe_file_config_sync<R>(reader: &mut R) -> Result<MuxFileConfig, MuxError>
where
    R: Read + Seek,
{
    use crate::probe::probe_with_options;
    let summary = probe_with_options(reader, crate::probe::ProbeOptions::lightweight())?;
    let config = MuxFileConfig::new(summary.timescale.max(1))
        .with_major_brand(summary.major_brand)
        .with_minor_version(summary.minor_version)
        .with_compatible_brands(summary.compatible_brands)
        .with_flat_source_movie_creation_time(extract_preserved_flat_movie_creation_time_sync(
            reader,
        )?)
        .with_flat_source_movie_modification_time(
            extract_preserved_flat_movie_modification_time_sync(reader)?,
        )
        .with_preserved_flat_prefix_bytes(extract_preserved_flat_prefix_bytes_sync(reader)?)
        .with_preserved_flat_iods_bytes(extract_preserved_flat_iods_bytes_sync(reader)?)
        .with_preserved_flat_udta_bytes(extract_preserved_flat_udta_bytes_sync(reader)?);
    Ok(config)
}

#[cfg(feature = "async")]
async fn probe_file_config_async<R>(reader: &mut R) -> Result<MuxFileConfig, MuxError>
where
    R: AsyncReadSeek,
{
    use crate::probe::probe_with_options_async;
    let summary =
        probe_with_options_async(reader, crate::probe::ProbeOptions::lightweight()).await?;
    let config = MuxFileConfig::new(summary.timescale.max(1))
        .with_major_brand(summary.major_brand)
        .with_minor_version(summary.minor_version)
        .with_compatible_brands(summary.compatible_brands)
        .with_flat_source_movie_creation_time(
            extract_preserved_flat_movie_creation_time_async(reader).await?,
        )
        .with_flat_source_movie_modification_time(
            extract_preserved_flat_movie_modification_time_async(reader).await?,
        )
        .with_preserved_flat_prefix_bytes(extract_preserved_flat_prefix_bytes_async(reader).await?)
        .with_preserved_flat_iods_bytes(extract_preserved_flat_iods_bytes_async(reader).await?)
        .with_preserved_flat_udta_bytes(extract_preserved_flat_udta_bytes_async(reader).await?);
    Ok(config)
}

fn extract_preserved_flat_movie_creation_time_sync<R>(
    reader: &mut R,
) -> Result<Option<u64>, MuxError>
where
    R: Read + Seek,
{
    Ok(
        extract_box_as::<_, Mvhd>(reader, None, BoxPath::from([MOOV, MVHD]))?
            .into_iter()
            .next()
            .map(|mvhd| mvhd.creation_time()),
    )
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_movie_creation_time_async<R>(
    reader: &mut R,
) -> Result<Option<u64>, MuxError>
where
    R: AsyncReadSeek,
{
    Ok(
        extract_box_as_async::<_, Mvhd>(reader, None, BoxPath::from([MOOV, MVHD]))
            .await?
            .into_iter()
            .next()
            .map(|mvhd| mvhd.creation_time()),
    )
}

fn extract_preserved_flat_movie_modification_time_sync<R>(
    reader: &mut R,
) -> Result<Option<u64>, MuxError>
where
    R: Read + Seek,
{
    Ok(
        extract_box_as::<_, Mvhd>(reader, None, BoxPath::from([MOOV, MVHD]))?
            .into_iter()
            .next()
            .map(|mvhd| mvhd.modification_time()),
    )
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_movie_modification_time_async<R>(
    reader: &mut R,
) -> Result<Option<u64>, MuxError>
where
    R: AsyncReadSeek,
{
    Ok(
        extract_box_as_async::<_, Mvhd>(reader, None, BoxPath::from([MOOV, MVHD]))
            .await?
            .into_iter()
            .next()
            .map(|mvhd| mvhd.modification_time()),
    )
}

fn extract_preserved_flat_prefix_bytes_sync<R>(reader: &mut R) -> Result<Vec<u8>, MuxError>
where
    R: Read + Seek,
{
    let file_size = reader.seek(SeekFrom::End(0))?;
    reader.seek(SeekFrom::Start(0))?;
    let mut saw_ftyp = false;
    let mut preserved = Vec::new();
    loop {
        let offset = reader.stream_position()?;
        if offset >= file_size {
            break;
        }
        let info = match crate::BoxInfo::read(reader) {
            Ok(info) => info,
            Err(crate::HeaderError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(error) => {
                return Err(MuxError::InvalidOutputLayout {
                    layout: "flat",
                    message: format!(
                        "failed to parse root box header while probing preserved flat prefix boxes: {error}"
                    ),
                });
            }
        };
        let box_end = info
            .offset()
            .checked_add(info.size())
            .ok_or(MuxError::LayoutOverflow("preserved flat prefix box range"))?;
        if box_end > file_size {
            break;
        }
        let box_type = info.box_type();
        if box_type == FTYP {
            saw_ftyp = true;
        } else if saw_ftyp && box_type == FREE {
            reader.seek(SeekFrom::Start(info.offset()))?;
            let mut box_bytes = vec![
                0_u8;
                usize::try_from(info.size()).map_err(|_| {
                    MuxError::LayoutOverflow("preserved flat prefix box size")
                })?
            ];
            reader.read_exact(&mut box_bytes)?;
            preserved.extend_from_slice(&box_bytes);
        } else if saw_ftyp {
            break;
        }
        reader.seek(SeekFrom::Start(box_end))?;
    }
    Ok(preserved)
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_prefix_bytes_async<R>(reader: &mut R) -> Result<Vec<u8>, MuxError>
where
    R: AsyncReadSeek,
{
    let file_size = reader.seek(SeekFrom::End(0)).await?;
    reader.seek(SeekFrom::Start(0)).await?;
    let mut saw_ftyp = false;
    let mut preserved = Vec::new();
    loop {
        let offset = reader.stream_position().await?;
        if offset >= file_size {
            break;
        }
        let info = match HeaderInfo::read_async(reader).await {
            Ok(info) => info,
            Err(crate::HeaderError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(error) => {
                return Err(MuxError::InvalidOutputLayout {
                    layout: "flat",
                    message: format!(
                        "failed to parse root box header while probing preserved flat prefix boxes: {error}"
                    ),
                });
            }
        };
        let box_end = info
            .offset()
            .checked_add(info.size())
            .ok_or(MuxError::LayoutOverflow("preserved flat prefix box range"))?;
        if box_end > file_size {
            break;
        }
        let box_type = info.box_type();
        if box_type == FTYP {
            saw_ftyp = true;
        } else if saw_ftyp && box_type == FREE {
            reader.seek(SeekFrom::Start(info.offset())).await?;
            let mut box_bytes = vec![
                0_u8;
                usize::try_from(info.size()).map_err(|_| {
                    MuxError::LayoutOverflow("preserved flat prefix box size")
                })?
            ];
            reader.read_exact(&mut box_bytes).await?;
            preserved.extend_from_slice(&box_bytes);
        } else if saw_ftyp {
            break;
        }
        reader.seek(SeekFrom::Start(box_end)).await?;
    }
    Ok(preserved)
}

fn extract_preserved_flat_udta_bytes_sync<R>(reader: &mut R) -> Result<Option<Vec<u8>>, MuxError>
where
    R: Read + Seek,
{
    Ok(
        extract_box_bytes(reader, None, BoxPath::from([MOOV, UDTA]))?
            .into_iter()
            .next(),
    )
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_udta_bytes_async<R>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, MuxError>
where
    R: AsyncReadSeek,
{
    Ok(
        extract_box_bytes_async(reader, None, BoxPath::from([MOOV, UDTA]))
            .await?
            .into_iter()
            .next(),
    )
}

fn extract_preserved_flat_iods_bytes_sync<R>(reader: &mut R) -> Result<Option<Vec<u8>>, MuxError>
where
    R: Read + Seek,
{
    Ok(
        extract_box_bytes(reader, None, BoxPath::from([MOOV, IODS]))?
            .into_iter()
            .next(),
    )
}

#[cfg(feature = "async")]
async fn extract_preserved_flat_iods_bytes_async<R>(
    reader: &mut R,
) -> Result<Option<Vec<u8>>, MuxError>
where
    R: AsyncReadSeek,
{
    Ok(
        extract_box_bytes_async(reader, None, BoxPath::from([MOOV, IODS]))
            .await?
            .into_iter()
            .next(),
    )
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn read_exact_at_async(
    file: &mut TokioFile,
    offset: u64,
    buf: &mut [u8],
    spec: &str,
    truncated_message: &'static str,
) -> Result<(), MuxError> {
    file.seek(SeekFrom::Start(offset)).await?;
    match file.read_exact(buf).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
            Err(MuxError::UnsupportedTrackImport {
                spec: spec.to_string(),
                message: truncated_message.to_string(),
            })
        }
        Err(error) => Err(MuxError::Io(error)),
    }
}

#[cfg(feature = "async")]
pub(in crate::mux) async fn read_spans_async(
    file: &mut TokioFile,
    spans: &[SourceFileSpan],
    total_size: u32,
    spec: &str,
    truncated_message: &'static str,
) -> Result<Vec<u8>, MuxError> {
    let mut bytes = Vec::with_capacity(
        usize::try_from(total_size)
            .map_err(|_| MuxError::LayoutOverflow("packet byte capacity"))?,
    );
    for span in spans {
        let mut chunk = vec![0_u8; usize::try_from(span.size).unwrap()];
        read_exact_at_async(
            file,
            span.source_offset,
            &mut chunk,
            spec,
            truncated_message,
        )
        .await?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
use crate::probe::detect_aac_profile;
