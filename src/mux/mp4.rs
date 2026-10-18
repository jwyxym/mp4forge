use std::collections::{BTreeMap, btree_map::Entry};
use std::fs::File;
use std::io::{Cursor, Read, Seek, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "async")]
use std::{io, pin::Pin, task::{Context, Poll}};

#[cfg(feature = "async")]
use tokio::fs::File as TokioFile;
#[cfg(feature = "async")]
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};

use crate::FourCc;
#[cfg(feature = "async")]
use crate::async_io::{AsyncReadSeek, AsyncWrite};
use crate::boxes::AnyTypeBox;
use crate::boxes::etsi_ts_102_366::Dec3;
use crate::boxes::iso14496_12::{
    AudioSampleEntry, Btrt, Co64, Ctts, CttsEntry, Dinf, Dref, Edts, Elst, ElstEntry, Emsg, Ftyp,
    Hdlr, Mdhd, Mdia, Mehd, Meta, Mfhd, Minf, Moof, Moov, Mvex, Mvhd, Nmhd, Pasp, Prft,
    SampleEntry, Sbgp, SbgpEntry, Sgpd, Sidx, SidxReference, Smhd, Stbl, Stco, Sthd, Stsc,
    StscEntry, Stsd, Stss, Stsz, Stts, SttsEntry, TFHD_DEFAULT_BASE_IS_MOOF,
    TFHD_DEFAULT_SAMPLE_DURATION_PRESENT, TFHD_DEFAULT_SAMPLE_FLAGS_PRESENT,
    TFHD_DEFAULT_SAMPLE_SIZE_PRESENT, TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT,
    TRUN_DATA_OFFSET_PRESENT, TRUN_SAMPLE_COMPOSITION_TIME_OFFSET_PRESENT,
    TRUN_SAMPLE_DURATION_PRESENT, TRUN_SAMPLE_FLAGS_PRESENT, TRUN_SAMPLE_SIZE_PRESENT, Tfdt, Tfhd,
    Tkhd, Traf, Trak, Trex, Trun, TrunEntry, Udta, Url, VisualRandomAccessEntry, VisualSampleEntry,
    Vmhd, split_box_children_with_optional_trailing_bytes,
};
use crate::boxes::iso14496_14::{
    Descriptor, ES_DESCRIPTOR_TAG, Esds, InitialObjectDescriptor, Iods,
};
use crate::boxes::metadata::{DATA_TYPE_STRING_UTF8, Data, Id32, Ilst, IlstMetaContainer};
use crate::codec::{CodecBox, ImmutableBox, MutableBox, marshal, unmarshal};
use crate::header::BoxInfo;
use crate::probe::{detect_aac_effective_sample_rate, detect_aac_profile};

#[cfg(feature = "async")]
use super::copy_planned_payloads_async;
use super::{
    MuxError, MuxFileConfig, MuxPlan, MuxTrackConfig, MuxTrackKind, copy_planned_payloads,
};

const IDENTITY_MATRIX: [i32; 9] = [0x0001_0000, 0, 0, 0, 0x0001_0000, 0, 0, 0, 0x4000_0000];
const VMHD_DEFAULT_FLAGS: u32 = 0x0000_0001;
const NON_KEY_SAMPLE_FLAGS: u32 = 0x0001_0000;
const CSLG: FourCc = FourCc::from_bytes(*b"cslg");
const SBGP: FourCc = FourCc::from_bytes(*b"sbgp");
const SGPD: FourCc = FourCc::from_bytes(*b"sgpd");
const SDSM: FourCc = FourCc::from_bytes(*b"sdsm");
const ISOM_UNIX_EPOCH_OFFSET: u64 = 2_082_844_800;
const AUTO_FLAT_MOVIE_TIMESCALE: u32 = 600;
const DEFAULT_FREE_PADDING_SIZE: usize = 67;
const FLAT_TOOL_METADATA_VALUE: &str =
    concat!(env!("CARGO_PKG_NAME"), " v", env!("CARGO_PKG_VERSION"));
const FRAGMENTED_ID3_OWNER: &str = env!("CARGO_PKG_REPOSITORY");
const FRAGMENTED_ID3_VALUE: &str = concat!(env!("CARGO_PKG_NAME"), " v", env!("CARGO_PKG_VERSION"));
const DEFAULT_FRAGMENTED_TKHD_FLAGS: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
const MAX_SIDX_REFERENCES: usize = u16::MAX as usize;

pub(super) fn write_mp4_mux<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    let layout = build_container_layout(file_config, track_configs, plan)?;
    writer.write_all(&layout.ftyp_bytes)?;
    if !layout.leading_bytes.is_empty() {
        writer.write_all(&layout.leading_bytes)?;
    }
    writer.write_all(&layout.moov_bytes)?;
    writer.write_all(&layout.mdat_header)?;
    copy_planned_payloads(sources, writer, plan)?;
    if !layout.trailing_bytes.is_empty() {
        writer.write_all(&layout.trailing_bytes)?;
    }
    writer.flush()?;
    Ok(())
}

pub(super) fn write_mp4_mux_to_path<P, Q>(
    source_paths: &[P],
    output_path: Q,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let mut sources = source_paths
        .iter()
        .map(File::open)
        .collect::<Result<Vec<_>, _>>()?;
    let mut writer = std::io::BufWriter::new(File::create(output_path)?);
    write_mp4_mux(&mut sources, &mut writer, file_config, track_configs, plan)
}

#[cfg(feature = "async")]
pub(super) async fn write_mp4_mux_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
	write_mp4_mux_async_with_progress(sources, writer, file_config, track_configs, plan, None).await
}

#[cfg(feature = "async")]
struct ProgressWriter<'a, W> {
	writer: &'a mut W,
	progress: Option<&'a (dyn Fn(u64, u64) + Send + Sync)>,
	written: u64,
	total: u64,
}

#[cfg(feature = "async")]
impl<W: AsyncWrite + Unpin> AsyncWrite for ProgressWriter<'_, W> {
	fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
		match Pin::new(&mut *self.writer).poll_write(cx, bytes) {
			Poll::Ready(Ok(count)) => {
				self.written += count as u64;
				if let Some(progress) = self.progress { progress(self.written, self.total); }
				Poll::Ready(Ok(count))
			}
			other => other,
		}
	}
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut *self.writer).poll_flush(cx)
	}
	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut *self.writer).poll_shutdown(cx)
	}
}

#[cfg(feature = "async")]
pub(super) async fn write_mp4_mux_async_with_progress<R, W>(
	sources: &mut [R],
	writer: &mut W,
	file_config: &MuxFileConfig,
	track_configs: &[MuxTrackConfig],
	plan: &MuxPlan,
	progress: Option<&(dyn Fn(u64, u64) + Send + Sync)>,
) -> Result<(), MuxError>
where
	R: AsyncReadSeek,
	W: AsyncWrite + Unpin,
{
    let layout = build_container_layout(file_config, track_configs, plan)?;
	let total: u64 = [layout.ftyp_bytes.len(), layout.leading_bytes.len(), layout.moov_bytes.len(),
		layout.mdat_header.len(), layout.trailing_bytes.len()].into_iter()
		.try_fold(plan.total_payload_size(), |total: u64, size: usize| total.checked_add(size as u64))
		.ok_or(MuxError::LayoutOverflow("MP4 output progress size"))?;
	let mut writer: ProgressWriter<'_, W> = ProgressWriter { writer, progress, written: 0, total };
	if let Some(progress) = progress { progress(0, total); }
    writer.write_all(&layout.ftyp_bytes).await?;
    if !layout.leading_bytes.is_empty() {
        writer.write_all(&layout.leading_bytes).await?;
    }
    writer.write_all(&layout.moov_bytes).await?;
    writer.write_all(&layout.mdat_header).await?;
    copy_planned_payloads_async(sources, &mut writer, plan).await?;
    if !layout.trailing_bytes.is_empty() {
        writer.write_all(&layout.trailing_bytes).await?;
    }
    writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
pub(super) async fn write_mp4_mux_to_path_async<P, Q>(
    source_paths: &[P],
    output_path: Q,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let mut sources = Vec::with_capacity(source_paths.len());
    for path in source_paths {
        sources.push(TokioFile::open(path).await?);
    }
    let output = TokioFile::create(output_path).await?;
    let mut writer = BufWriter::new(output);
    write_mp4_mux_async(&mut sources, &mut writer, file_config, track_configs, plan).await
}

pub(super) fn write_fragmented_mp4_mux<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init(&layout, writer)?;
    write_fragmented_media(sources, writer, &layout, false)?;
    writer.flush()?;
    Ok(())
}

pub(super) fn write_fragmented_mp4_mux_split<R, I, M>(
    sources: &mut [R],
    init_writer: &mut I,
    media_writer: &mut M,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    I: Write,
    M: Write,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init(&layout, init_writer)?;
    init_writer.flush()?;
    write_fragmented_media(sources, media_writer, &layout, false)?;
    media_writer.flush()?;
    Ok(())
}

pub(super) fn write_fragmented_mp4_mux_segmented<R, I, M>(
    sources: &mut [R],
    init_writer: &mut I,
    media_writer: &mut M,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    I: Write,
    M: Write,
{
    let layout = build_fragmented_segmented_layout(file_config, track_configs, plan)?;
    write_fragmented_init(&layout, init_writer)?;
    init_writer.flush()?;
    write_fragmented_media(sources, media_writer, &layout, false)?;
    media_writer.flush()?;
    Ok(())
}

pub(super) fn write_fragmented_mp4_mux_chunked<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init(&layout, writer)?;
    write_fragmented_media(sources, writer, &layout, true)?;
    Ok(())
}

fn write_fragmented_init<W>(layout: &FragmentedLayout, writer: &mut W) -> Result<(), MuxError>
where
    W: Write,
{
    writer.write_all(&layout.ftyp_bytes)?;
    writer.write_all(&layout.moov_bytes)?;
    Ok(())
}

fn write_fragmented_media<R, W>(
    sources: &mut [R],
    writer: &mut W,
    layout: &FragmentedLayout,
    flush_each_fragment: bool,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    writer.write_all(&layout.sidx_bytes)?;
    if flush_each_fragment {
        writer.flush()?;
    }
    for (fragment_index, fragment) in layout.fragments.iter().enumerate() {
        write_fragmented_media_fragment(
            sources,
            writer,
            fragment,
            layout
                .segment_index_bytes
                .get(fragment_index)
                .map(Vec::as_slice),
        )?;
        if flush_each_fragment {
            writer.flush()?;
        }
    }
    Ok(())
}

fn write_fragmented_media_fragment<R, W>(
    sources: &mut [R],
    writer: &mut W,
    fragment: &FragmentLayout,
    segment_index_bytes: Option<&[u8]>,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    writer.write_all(&fragment.segment_type_bytes)?;
    if let Some(segment_index_bytes) = segment_index_bytes {
        writer.write_all(segment_index_bytes)?;
    }
    for metadata_bytes in &fragment.metadata_bytes {
        writer.write_all(metadata_bytes)?;
    }
    writer.write_all(&fragment.moof_bytes)?;
    writer.write_all(&fragment.mdat_header)?;
    copy_fragment_payloads(sources, writer, fragment)?;
    Ok(())
}

#[cfg(feature = "async")]
pub(super) async fn write_fragmented_mp4_mux_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init_async(&layout, writer).await?;
    write_fragmented_media_async(sources, writer, &layout, false).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
pub(super) async fn write_fragmented_mp4_mux_split_async<R, I, M>(
    sources: &mut [R],
    init_writer: &mut I,
    media_writer: &mut M,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    I: AsyncWrite + Unpin,
    M: AsyncWrite + Unpin,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init_async(&layout, init_writer).await?;
    init_writer.flush().await?;
    write_fragmented_media_async(sources, media_writer, &layout, false).await?;
    media_writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
pub(super) async fn write_fragmented_mp4_mux_segmented_async<R, I, M>(
    sources: &mut [R],
    init_writer: &mut I,
    media_writer: &mut M,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    I: AsyncWrite + Unpin,
    M: AsyncWrite + Unpin,
{
    let layout = build_fragmented_segmented_layout(file_config, track_configs, plan)?;
    write_fragmented_init_async(&layout, init_writer).await?;
    init_writer.flush().await?;
    write_fragmented_media_async(sources, media_writer, &layout, false).await?;
    media_writer.flush().await?;
    Ok(())
}

#[cfg(feature = "async")]
pub(super) async fn write_fragmented_mp4_mux_chunked_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
    let layout = build_fragmented_layout(file_config, track_configs, single_sidx_reference, plan)?;
    write_fragmented_init_async(&layout, writer).await?;
    write_fragmented_media_async(sources, writer, &layout, true).await?;
    Ok(())
}

#[cfg(feature = "async")]
async fn write_fragmented_init_async<W>(
    layout: &FragmentedLayout,
    writer: &mut W,
) -> Result<(), MuxError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(&layout.ftyp_bytes).await?;
    writer.write_all(&layout.moov_bytes).await?;
    Ok(())
}

#[cfg(feature = "async")]
async fn write_fragmented_media_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    layout: &FragmentedLayout,
    flush_each_fragment: bool,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
    writer.write_all(&layout.sidx_bytes).await?;
    if flush_each_fragment {
        writer.flush().await?;
    }
    for (fragment_index, fragment) in layout.fragments.iter().enumerate() {
        write_fragmented_media_fragment_async(
            sources,
            writer,
            fragment,
            layout
                .segment_index_bytes
                .get(fragment_index)
                .map(Vec::as_slice),
        )
        .await?;
        if flush_each_fragment {
            writer.flush().await?;
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn write_fragmented_media_fragment_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    fragment: &FragmentLayout,
    segment_index_bytes: Option<&[u8]>,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
    writer.write_all(&fragment.segment_type_bytes).await?;
    if let Some(segment_index_bytes) = segment_index_bytes {
        writer.write_all(segment_index_bytes).await?;
    }
    for metadata_bytes in &fragment.metadata_bytes {
        writer.write_all(metadata_bytes).await?;
    }
    writer.write_all(&fragment.moof_bytes).await?;
    writer.write_all(&fragment.mdat_header).await?;
    copy_fragment_payloads_async(sources, writer, fragment).await?;
    Ok(())
}

struct ContainerLayout {
    ftyp_bytes: Vec<u8>,
    leading_bytes: Vec<u8>,
    moov_bytes: Vec<u8>,
    mdat_header: Vec<u8>,
    trailing_bytes: Vec<u8>,
}

struct FragmentedLayout {
    ftyp_bytes: Vec<u8>,
    moov_bytes: Vec<u8>,
    sidx_bytes: Vec<u8>,
    segment_index_bytes: Vec<Vec<u8>>,
    fragments: Vec<FragmentLayout>,
}

struct FragmentLayout {
    segment_type_bytes: Vec<u8>,
    metadata_bytes: Vec<Vec<u8>>,
    moof_bytes: Vec<u8>,
    mdat_header: Vec<u8>,
    samples: Vec<PreparedSample>,
    sidx_samples: Vec<PreparedSample>,
}

struct FragmentTrackRun<'a> {
    track: &'a PreparedTrack<'a>,
    samples: Vec<PreparedSample>,
    payload_offset: u64,
}

struct BuiltSidxReference {
    reference: SidxReference,
    earliest_presentation_time: u64,
}

type SampleEntryChildBoxes = Vec<Vec<u8>>;
type SampleEntryTrailingBytes = Vec<u8>;
type SampleEntryParts<T> = (T, SampleEntryChildBoxes, SampleEntryTrailingBytes);

struct PreparedTrack<'a> {
    config: &'a MuxTrackConfig,
    sample_entry_box: &'a [u8],
    samples: Vec<PreparedSample>,
    chunk_sample_counts: Vec<u32>,
    fragmented_reference_group_fragment_counts: Option<Vec<u32>>,
    media_duration: u64,
    presentation_duration_media: u64,
    edit_media_time: Option<u64>,
    flat_timing_override: Option<&'a super::FlatTimingOverride>,
}

#[derive(Clone, Copy)]
struct PreparedSample {
    source_index: usize,
    source_data_offset: u64,
    decode_time_movie: u64,
    decode_time_media: u64,
    output_offset: u64,
    sample_size: u64,
    duration_movie: u32,
    duration_media: u32,
    composition_offset_movie: i32,
    composition_offset_media: i32,
    is_sync_sample: bool,
    sample_description_index: u32,
}

fn build_container_layout(
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<ContainerLayout, MuxError> {
    if file_config.movie_timescale() == 0 {
        return Err(MuxError::InvalidMovieTimescale);
    }

    let prepared_tracks = prepare_tracks(file_config, track_configs, plan, true)?;
    let ftyp_bytes = build_ftyp_bytes(file_config, &prepared_tracks)?;
    let leading_bytes = file_config.preserved_flat_prefix_bytes().to_vec();
    let ftyp_size =
        u64::try_from(ftyp_bytes.len()).map_err(|_| MuxError::LayoutOverflow("ftyp size"))?;
    let leading_bytes_size = u64::try_from(leading_bytes.len())
        .map_err(|_| MuxError::LayoutOverflow("leading box size"))?;
    let moov_start = ftyp_size
        .checked_add(leading_bytes_size)
        .ok_or(MuxError::LayoutOverflow("moov start"))?;
    let mdat_header = encode_header_only(
        FourCc::from_bytes(*b"mdat"),
        plan.total_payload_size(),
        "mdat header",
    )?;
    let mdat_header_size =
        u64::try_from(mdat_header.len()).map_err(|_| MuxError::LayoutOverflow("mdat header"))?;
    let provisional_moov = build_moov_bytes(
        file_config,
        &prepared_tracks,
        moov_start,
        mdat_header_size,
        0,
    )?;
    let moov_size =
        u64::try_from(provisional_moov.len()).map_err(|_| MuxError::LayoutOverflow("moov size"))?;
    let mdat_data_start = moov_start
        .checked_add(moov_size)
        .and_then(|offset| offset.checked_add(mdat_header_size))
        .ok_or(MuxError::LayoutOverflow("mdat data start"))?;
    let moov_bytes = build_moov_bytes(
        file_config,
        &prepared_tracks,
        moov_start,
        mdat_header_size,
        mdat_data_start,
    )?;
    let trailing_bytes = if file_config.auto_flat_profile() || file_config.keep_flat_free_box() {
        build_free_padding_bytes(file_config)?
    } else {
        Vec::new()
    };

    if moov_bytes.len() != provisional_moov.len() {
        return Err(MuxError::LayoutOverflow(
            "moov size changed after chunk-offset resolution",
        ));
    }

    Ok(ContainerLayout {
        ftyp_bytes,
        leading_bytes,
        moov_bytes,
        mdat_header,
        trailing_bytes,
    })
}

fn build_fragmented_layout(
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    plan: &MuxPlan,
) -> Result<FragmentedLayout, MuxError> {
    build_fragmented_layout_with_media_mode(
        file_config,
        track_configs,
        single_sidx_reference,
        FragmentedMediaMode::SingleFile,
        plan,
    )
}

fn build_fragmented_segmented_layout(
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    plan: &MuxPlan,
) -> Result<FragmentedLayout, MuxError> {
    build_fragmented_layout_with_media_mode(
        file_config,
        track_configs,
        false,
        FragmentedMediaMode::StandaloneSegments,
        plan,
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FragmentedMediaMode {
    SingleFile,
    StandaloneSegments,
}

fn build_fragmented_layout_with_media_mode(
    file_config: &MuxFileConfig,
    track_configs: &[MuxTrackConfig],
    single_sidx_reference: bool,
    media_mode: FragmentedMediaMode,
    plan: &MuxPlan,
) -> Result<FragmentedLayout, MuxError> {
    if file_config.movie_timescale() == 0 {
        return Err(MuxError::InvalidMovieTimescale);
    }

    let prepared_tracks = prepare_tracks(file_config, track_configs, plan, false)?;
    let mut fragment_layouts = build_fragment_layouts(file_config, &prepared_tracks)?;
    let ftyp = build_fragmented_ftyp(&prepared_tracks)?;
    let ftyp_bytes = encode_typed_box(&ftyp, &[])?;
    if media_mode == FragmentedMediaMode::StandaloneSegments {
        let segment_type_bytes = build_fragmented_segment_type_bytes(&ftyp)?;
        for fragment in &mut fragment_layouts {
            fragment.segment_type_bytes = segment_type_bytes.clone();
        }
    }
    let moov_bytes = build_fragmented_moov_bytes(file_config, &prepared_tracks)?;
    let sidx_track = select_fragmented_sidx_track(&prepared_tracks)?;
    let (sidx_bytes, segment_index_bytes) = match media_mode {
        FragmentedMediaMode::SingleFile => (
            build_sidx_bytes(
                file_config,
                sidx_track,
                &fragment_layouts,
                single_sidx_reference,
            )?,
            Vec::new(),
        ),
        FragmentedMediaMode::StandaloneSegments => (
            Vec::new(),
            build_fragment_segment_index_bytes(file_config, sidx_track, &fragment_layouts)?,
        ),
    };

    Ok(FragmentedLayout {
        ftyp_bytes,
        moov_bytes,
        sidx_bytes,
        segment_index_bytes,
        fragments: fragment_layouts,
    })
}

fn build_fragmented_ftyp(tracks: &[PreparedTrack<'_>]) -> Result<Ftyp, MuxError> {
    let mut compatible_brands = vec![
        FourCc::from_bytes(*b"iso8"),
        FourCc::from_bytes(*b"isom"),
        FourCc::from_bytes(*b"mp41"),
        FourCc::from_bytes(*b"dash"),
    ];
    for track in tracks {
        for sample_entry_box in track.config.sample_entry_boxes() {
            append_fragmented_sample_entry_brands(sample_entry_box, &mut compatible_brands)?;
        }
    }

    Ok(Ftyp {
        major_brand: FourCc::from_bytes(*b"mp41"),
        minor_version: 0,
        compatible_brands,
    })
}

fn build_fragmented_segment_type_bytes(ftyp: &Ftyp) -> Result<Vec<u8>, MuxError> {
    let mut segment_type = ftyp.clone();
    for brand in &mut segment_type.compatible_brands {
        if *brand == FourCc::from_bytes(*b"cmfc") {
            *brand = FourCc::from_bytes(*b"cmfs");
        }
    }
    encode_typed_box_as(FourCc::from_bytes(*b"styp"), &segment_type, &[])
}

fn push_unique_brand(compatible_brands: &mut Vec<FourCc>, brand: FourCc) {
    if !compatible_brands.contains(&brand) {
        compatible_brands.push(brand);
    }
}

fn append_fragmented_sample_entry_brands(
    sample_entry_box: &[u8],
    compatible_brands: &mut Vec<FourCc>,
) -> Result<(), MuxError> {
    let sample_entry_type = sample_entry_box_type(sample_entry_box)?;
    let carries_dolby_vision_config = sample_entry_carries_child_type(
        sample_entry_box,
        &[FourCc::from_bytes(*b"dvcC"), FourCc::from_bytes(*b"dvvC")],
    );
    match sample_entry_type {
        value
            if value == FourCc::from_bytes(*b"avc1")
                || value == FourCc::from_bytes(*b"avc2")
                || value == FourCc::from_bytes(*b"avc3")
                || value == FourCc::from_bytes(*b"avc4") =>
        {
            push_unique_brand(compatible_brands, value);
            if value == FourCc::from_bytes(*b"avc1") {
                push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
            }
        }
        value
            if matches!(
                value,
                _
                    if value == FourCc::from_bytes(*b"hvc1")
                        || value == FourCc::from_bytes(*b"hev1")
                        || value == FourCc::from_bytes(*b"dvh1")
                        || value == FourCc::from_bytes(*b"dvhe")
            ) =>
        {
            push_unique_brand(compatible_brands, value);
            if value == FourCc::from_bytes(*b"dvh1")
                || value == FourCc::from_bytes(*b"dvhe")
                || carries_dolby_vision_config
            {
                push_unique_brand(compatible_brands, FourCc::from_bytes(*b"dby1"));
            }
            if value != FourCc::from_bytes(*b"hev1") {
                push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
            }
        }
        value if value == FourCc::from_bytes(*b"vvc1") || value == FourCc::from_bytes(*b"vvi1") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"vvc1"));
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
        value if value == FourCc::from_bytes(*b"av01") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"av01"));
            if carries_dolby_vision_config {
                push_unique_brand(compatible_brands, FourCc::from_bytes(*b"dby1"));
            }
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
        value if value == FourCc::from_bytes(*b"vp08") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"vp08"));
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
        value if value == FourCc::from_bytes(*b"vp09") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"vp09"));
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
        value if value == FourCc::from_bytes(*b"vp10") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"vp10"));
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
        value if value == FourCc::from_bytes(*b"iamf") => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"iamf"));
        }
        _ => {
            push_unique_brand(compatible_brands, FourCc::from_bytes(*b"cmfc"));
        }
    }
    Ok(())
}

fn build_fragment_layouts(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
) -> Result<Vec<FragmentLayout>, MuxError> {
    let mut fragments = Vec::new();
    let Some(sidx_track) = select_fragmented_sidx_track(tracks).ok() else {
        return Ok(fragments);
    };
    let fragment_count = tracks
        .iter()
        .map(|track| track.chunk_sample_counts.len())
        .max()
        .unwrap_or(0);
    let mut sample_indices = vec![0_usize; tracks.len()];
    for fragment_index in 0..fragment_count {
        let mut runs = Vec::new();
        let mut all_samples = Vec::new();
        let mut sidx_samples = Vec::new();
        let mut payload_size = 0_u64;
        for (track_index, track) in tracks.iter().enumerate() {
            let Some(&samples_per_chunk) = track.chunk_sample_counts.get(fragment_index) else {
                continue;
            };
            let sample_count = usize::try_from(samples_per_chunk)
                .map_err(|_| MuxError::LayoutOverflow("fragment sample count"))?;
            let sample_index = sample_indices[track_index];
            let end_index = sample_index
                .checked_add(sample_count)
                .ok_or(MuxError::LayoutOverflow("fragment sample indexing"))?;
            let fragment_samples = track
                .samples
                .get(sample_index..end_index)
                .ok_or_else(|| MuxError::InvalidChunkPlan {
                    track_id: track.config.track_id(),
                    message: "fragment boundaries ran past the staged sample count".to_string(),
                })?
                .to_vec();
            let track_payload_size = fragment_samples.iter().try_fold(0_u64, |total, sample| {
                total
                    .checked_add(sample.sample_size)
                    .ok_or(MuxError::LayoutOverflow("fragment payload size"))
            })?;
            if track.config.track_id() == sidx_track.config.track_id() {
                sidx_samples = fragment_samples.clone();
            }
            all_samples.extend(fragment_samples.iter().copied());
            let mut payload_offset = payload_size;
            for fragment_run_samples in split_sample_description_runs(&fragment_samples) {
                let run_payload_size =
                    fragment_run_samples
                        .iter()
                        .try_fold(0_u64, |total, sample| {
                            total
                                .checked_add(sample.sample_size)
                                .ok_or(MuxError::LayoutOverflow("fragment payload size"))
                        })?;
                runs.push(FragmentTrackRun {
                    track,
                    samples: fragment_run_samples.to_vec(),
                    payload_offset,
                });
                payload_offset = payload_offset
                    .checked_add(run_payload_size)
                    .ok_or(MuxError::LayoutOverflow("fragment payload size"))?;
            }
            payload_size = payload_size
                .checked_add(track_payload_size)
                .ok_or(MuxError::LayoutOverflow("fragment payload size"))?;
            sample_indices[track_index] = end_index;
        }
        if runs.is_empty() {
            continue;
        }
        let mdat_header = encode_header_only(FourCc::from_bytes(*b"mdat"), payload_size, "mdat")?;
        let moof_bytes = build_fragment_moof_bytes(
            &runs,
            mdat_header.len(),
            u32::try_from(fragment_index + 1)
                .map_err(|_| MuxError::LayoutOverflow("fragment sequence number"))?,
        )?;
        let metadata_bytes = build_fragment_metadata_bytes(file_config, fragment_index)?;
        fragments.push(FragmentLayout {
            segment_type_bytes: Vec::new(),
            metadata_bytes,
            moof_bytes,
            mdat_header,
            samples: all_samples,
            sidx_samples,
        });
    }
    for (track_index, track) in tracks.iter().enumerate() {
        if sample_indices[track_index] != track.samples.len() {
            return Err(MuxError::InvalidChunkPlan {
                track_id: track.config.track_id(),
                message: "fragment boundaries did not cover every staged sample".to_string(),
            });
        }
    }
    Ok(fragments)
}

fn split_sample_description_runs(samples: &[PreparedSample]) -> Vec<&[PreparedSample]> {
    if samples.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut start = 0_usize;
    for index in 1..samples.len() {
        if samples[index].sample_description_index != samples[index - 1].sample_description_index {
            runs.push(&samples[start..index]);
            start = index;
        }
    }
    runs.push(&samples[start..]);
    runs
}

fn build_fragment_metadata_bytes(
    file_config: &MuxFileConfig,
    fragment_index: usize,
) -> Result<Vec<Vec<u8>>, MuxError> {
    let fragment_index = u32::try_from(fragment_index)
        .map_err(|_| MuxError::LayoutOverflow("fragment metadata index"))?;
    let mut boxes = Vec::new();
    for message in file_config
        .fragment_event_messages()
        .iter()
        .filter(|message| message.fragment_index() == fragment_index)
    {
        boxes.push(build_fragment_event_message_bytes(message)?);
    }
    for entry in file_config
        .producer_reference_times()
        .iter()
        .filter(|entry| entry.fragment_index() == fragment_index)
    {
        boxes.push(build_producer_reference_time_bytes(entry)?);
    }
    Ok(boxes)
}

fn build_fragment_event_message_bytes(
    message: &super::MuxFragmentEventMessage,
) -> Result<Vec<u8>, MuxError> {
    if message.timescale() == 0 {
        return Err(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: "fragment event message timescale must be greater than zero".to_string(),
        });
    }
    let mut emsg = Emsg::default();
    emsg.scheme_id_uri = message.scheme_id_uri().to_string();
    emsg.value = message.value().to_string();
    emsg.timescale = message.timescale();
    emsg.presentation_time_delta = message.presentation_time_delta();
    emsg.presentation_time = message.presentation_time();
    emsg.event_duration = message.event_duration();
    emsg.id = message.id();
    emsg.message_data = message.message_data().to_vec();
    match message.version() {
        0 | 1 => emsg.set_version(message.version()),
        version => {
            return Err(MuxError::InvalidOutputLayout {
                layout: "fragmented",
                message: format!("fragment event message version {version} is not supported"),
            });
        }
    }
    encode_typed_box(&emsg, &[])
}

fn build_producer_reference_time_bytes(
    entry: &super::MuxProducerReferenceTime,
) -> Result<Vec<u8>, MuxError> {
    if entry.reference_track_id() == 0 {
        return Err(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: "producer reference time requires a nonzero reference track id".to_string(),
        });
    }
    if entry.flags() > 0x00ff_ffff {
        return Err(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: "producer reference time flags must fit in 24 bits".to_string(),
        });
    }
    let mut prft = Prft::default();
    prft.reference_track_id = entry.reference_track_id();
    prft.ntp_timestamp = entry.ntp_timestamp();
    match entry.version() {
        0 => {
            prft.set_version(0);
            prft.media_time_v0 =
                u32::try_from(entry.media_time()).map_err(|_| MuxError::InvalidOutputLayout {
                    layout: "fragmented",
                    message: "producer reference time version 0 requires media time to fit in u32"
                        .to_string(),
                })?;
        }
        1 => {
            prft.set_version(1);
            prft.media_time_v1 = entry.media_time();
        }
        version => {
            return Err(MuxError::InvalidOutputLayout {
                layout: "fragmented",
                message: format!("producer reference time version {version} is not supported"),
            });
        }
    }
    prft.set_flags(entry.flags());
    encode_typed_box(&prft, &[])
}

fn select_fragmented_sidx_track<'a>(
    tracks: &'a [PreparedTrack<'a>],
) -> Result<&'a PreparedTrack<'a>, MuxError> {
    let fragment_count = tracks
        .iter()
        .map(|track| track.chunk_sample_counts.len())
        .max()
        .unwrap_or(0);
    tracks
        .iter()
        .find(|track| {
            track.config.kind() == MuxTrackKind::Video
                && !track.samples.is_empty()
                && track.chunk_sample_counts.len() == fragment_count
        })
        .or_else(|| {
            tracks.iter().find(|track| {
                !track.samples.is_empty() && track.chunk_sample_counts.len() == fragment_count
            })
        })
        .or_else(|| {
            tracks.iter().find(|track| {
                track.config.kind() == MuxTrackKind::Video && !track.samples.is_empty()
            })
        })
        .or_else(|| tracks.iter().find(|track| !track.samples.is_empty()))
        .ok_or(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: "fragmented output requires at least one prepared track with samples"
                .to_string(),
        })
}

fn build_fragmented_moov_bytes(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
) -> Result<Vec<u8>, MuxError> {
    let fragmented_creation_time = current_isom_time()?;
    let mvhd = build_fragmented_mvhd(file_config, tracks, fragmented_creation_time)?;
    let mut children = vec![
        encode_typed_box(&mvhd, &[])?,
        build_fragmented_meta_bytes()?,
    ];
    for track in tracks {
        children.push(build_fragmented_trak_bytes(
            track,
            fragmented_creation_time,
        )?);
    }
    children.push(build_mvex_bytes(file_config.movie_timescale(), tracks)?);
    encode_typed_box(&Moov, &children.concat())
}

fn build_fragmented_meta_bytes() -> Result<Vec<u8>, MuxError> {
    let mut id32 = Id32::default();
    id32.language = "eng".to_string();
    id32.id3v2_data = build_fragmented_identity_id3_payload();

    let children = [
        build_fragmented_identity_hdlr_bytes()?,
        encode_typed_box(&id32, &[])?,
    ]
    .concat();
    encode_typed_box(&Meta::default(), &children)
}

fn build_fragmented_identity_hdlr_bytes() -> Result<Vec<u8>, MuxError> {
    let mut payload = Vec::with_capacity(24);
    payload.extend_from_slice(&[0, 0, 0, 0]);
    payload.extend_from_slice(&[0, 0, 0, 0]);
    payload.extend_from_slice(FourCc::from_bytes(*b"ID32").as_bytes().as_ref());
    payload.extend_from_slice(&[0; 12]);

    let mut bytes = encode_header_only(
        FourCc::from_bytes(*b"hdlr"),
        u64::try_from(payload.len())
            .map_err(|_| MuxError::LayoutOverflow("fragmented meta hdlr"))?,
        "fragmented meta hdlr",
    )?;
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn build_fragmented_identity_id3_payload() -> Vec<u8> {
    let mut frame_payload =
        Vec::with_capacity(FRAGMENTED_ID3_OWNER.len() + 1 + FRAGMENTED_ID3_VALUE.len());
    frame_payload.extend_from_slice(FRAGMENTED_ID3_OWNER.as_bytes());
    frame_payload.push(0);
    frame_payload.extend_from_slice(FRAGMENTED_ID3_VALUE.as_bytes());

    let mut priv_frame = Vec::with_capacity(10 + frame_payload.len());
    priv_frame.extend_from_slice(b"PRIV");
    priv_frame.extend_from_slice(&encode_synchsafe_u32(frame_payload.len() as u32));
    priv_frame.extend_from_slice(&[0, 0]);
    priv_frame.extend_from_slice(&frame_payload);

    let mut id3 = Vec::with_capacity(10 + priv_frame.len());
    id3.extend_from_slice(b"ID3");
    id3.extend_from_slice(&[4, 0, 0]);
    id3.extend_from_slice(&encode_synchsafe_u32(priv_frame.len() as u32));
    id3.extend_from_slice(&priv_frame);
    id3
}

fn encode_synchsafe_u32(value: u32) -> [u8; 4] {
    [
        ((value >> 21) & 0x7F) as u8,
        ((value >> 14) & 0x7F) as u8,
        ((value >> 7) & 0x7F) as u8,
        (value & 0x7F) as u8,
    ]
}

fn build_fragmented_mvhd(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
    fragmented_creation_time: u32,
) -> Result<Mvhd, MuxError> {
    let mut mvhd = build_mvhd(file_config, tracks, None)?;
    mvhd.set_version(0);
    mvhd.creation_time_v0 = fragmented_creation_time;
    mvhd.modification_time_v0 = fragmented_creation_time;
    mvhd.creation_time_v1 = 0;
    mvhd.modification_time_v1 = 0;
    mvhd.duration_v0 = 0;
    mvhd.duration_v1 = 0;
    Ok(mvhd)
}

fn build_fragmented_trak_bytes(
    track: &PreparedTrack<'_>,
    fragmented_creation_time: u32,
) -> Result<Vec<u8>, MuxError> {
    let tkhd = build_fragmented_tkhd(track, fragmented_creation_time)?;
    let mdia = build_fragmented_mdia_bytes(track, fragmented_creation_time)?;
    let mut children = vec![encode_typed_box(&tkhd, &[])?, mdia];
    if let Some(edts) = build_edts_bytes(track, 0)? {
        children.push(edts);
    }
    encode_typed_box(&Trak, &children.concat())
}

fn build_fragmented_tkhd(
    track: &PreparedTrack<'_>,
    fragmented_creation_time: u32,
) -> Result<Tkhd, MuxError> {
    let mut tkhd = build_tkhd_with_movie_timescale(track, track.config.timescale())?;
    tkhd.set_flags(DEFAULT_FRAGMENTED_TKHD_FLAGS);
    tkhd.alternate_group = 0;
    tkhd.volume = match track.config.kind() {
        MuxTrackKind::Audio => 0x0100,
        MuxTrackKind::Video | MuxTrackKind::Text | MuxTrackKind::Subtitle => 0,
    };
    tkhd.matrix = IDENTITY_MATRIX;
    tkhd.set_version(0);
    tkhd.creation_time_v0 = fragmented_creation_time;
    tkhd.modification_time_v0 = fragmented_creation_time;
    tkhd.creation_time_v1 = 0;
    tkhd.modification_time_v1 = 0;
    tkhd.duration_v0 = 0;
    tkhd.duration_v1 = 0;
    Ok(tkhd)
}

fn build_edts_bytes(
    track: &PreparedTrack<'_>,
    segment_duration: u64,
) -> Result<Option<Vec<u8>>, MuxError> {
    let Some(edit_media_time) = track.edit_media_time else {
        return Ok(None);
    };
    let mut elst = Elst::default();
    elst.entry_count = 1;
    if edit_media_time > u64::try_from(i32::MAX).unwrap_or(u64::MAX)
        || segment_duration > u64::from(u32::MAX)
    {
        elst.set_version(1);
        elst.entries.push(ElstEntry {
            segment_duration_v1: segment_duration,
            media_time_v1: i64::try_from(edit_media_time)
                .map_err(|_| MuxError::LayoutOverflow("fragmented edit-list media time"))?,
            media_rate_integer: 1,
            ..ElstEntry::default()
        });
    } else {
        elst.entries.push(ElstEntry {
            segment_duration_v0: u32::try_from(segment_duration)
                .map_err(|_| MuxError::LayoutOverflow("edit-list segment duration"))?,
            media_time_v0: i32::try_from(edit_media_time)
                .map_err(|_| MuxError::LayoutOverflow("fragmented edit-list media time"))?,
            media_rate_integer: 1,
            ..ElstEntry::default()
        });
    }
    Ok(Some(encode_typed_box(
        &Edts,
        &encode_typed_box(&elst, &[])?,
    )?))
}

fn build_fragmented_mdia_bytes(
    track: &PreparedTrack<'_>,
    fragmented_creation_time: u32,
) -> Result<Vec<u8>, MuxError> {
    let mdhd = build_fragmented_mdhd(track, fragmented_creation_time)?;
    let hdlr = build_hdlr(track);
    let minf = build_fragmented_minf_bytes(track)?;
    let children = [
        encode_typed_box(&mdhd, &[])?,
        encode_typed_box(&hdlr, &[])?,
        minf,
    ]
    .concat();
    encode_typed_box(&Mdia, &children)
}

fn build_fragmented_mdhd(
    track: &PreparedTrack<'_>,
    fragmented_creation_time: u32,
) -> Result<Mdhd, MuxError> {
    let mut mdhd = build_mdhd_base(track)?;
    mdhd.set_version(0);
    mdhd.creation_time_v0 = fragmented_creation_time;
    mdhd.modification_time_v0 = fragmented_creation_time;
    mdhd.creation_time_v1 = 0;
    mdhd.modification_time_v1 = 0;
    mdhd.duration_v0 = 0;
    mdhd.duration_v1 = 0;
    Ok(mdhd)
}

fn build_fragmented_minf_bytes(track: &PreparedTrack<'_>) -> Result<Vec<u8>, MuxError> {
    let media_header = match track.config.kind() {
        MuxTrackKind::Audio => encode_typed_box(&Smhd::default(), &[])?,
        MuxTrackKind::Video => {
            let mut vmhd = Vmhd::default();
            vmhd.set_flags(VMHD_DEFAULT_FLAGS);
            encode_typed_box(&vmhd, &[])?
        }
        MuxTrackKind::Text => encode_typed_box(&Nmhd::default(), &[])?,
        MuxTrackKind::Subtitle => {
            build_subtitle_media_header_bytes(track.config.sample_entry_box())?
        }
    };
    let dinf = build_dinf_bytes()?;
    let stbl = build_fragmented_stbl_bytes(track)?;
    encode_typed_box(&Minf, &[dinf, stbl, media_header].concat())
}

fn build_fragmented_stbl_bytes(track: &PreparedTrack<'_>) -> Result<Vec<u8>, MuxError> {
    let stsd = build_fragmented_stsd_bytes(track)?;
    let mut stts = Stts::default();
    stts.entry_count = 0;
    let mut stsc = Stsc::default();
    stsc.entry_count = 0;
    let mut stsz = Stsz::default();
    stsz.sample_size = 0;
    stsz.sample_count = 0;
    let mut stco = Stco::default();
    stco.entry_count = 0;
    let mut children = vec![
        stsd,
        encode_typed_box(&stts, &[])?,
        encode_typed_box(&stsc, &[])?,
        encode_typed_box(&stsz, &[])?,
        encode_typed_box(&stco, &[])?,
    ];
    if fragmented_track_emits_roll_description(track)
        && let Some(sample_roll_distance) = track.config.sample_roll_distance()
    {
        children.push(encode_typed_box(
            &build_roll_sgpd(sample_roll_distance),
            &[],
        )?);
    }
    encode_typed_box(&Stbl, &children.concat())
}

fn build_fragmented_stsd_bytes(track: &PreparedTrack<'_>) -> Result<Vec<u8>, MuxError> {
    let mut stsd = Stsd::default();
    stsd.entry_count = u32::try_from(track.config.sample_entry_boxes().len())
        .map_err(|_| MuxError::LayoutOverflow("stsd entry_count"))?;
    let mut sample_entry_boxes = Vec::new();
    for sample_entry_box in track.config.sample_entry_boxes() {
        sample_entry_boxes.extend(canonicalize_fragmented_sample_entry_box(sample_entry_box)?);
    }
    encode_typed_box(&stsd, &sample_entry_boxes)
}

fn build_mvex_bytes(
    movie_timescale: u32,
    tracks: &[PreparedTrack<'_>],
) -> Result<Vec<u8>, MuxError> {
    let mut children = Vec::new();
    let mut fragment_duration = 0_u64;
    for track in tracks {
        fragment_duration =
            fragment_duration.max(fragmented_mehd_duration(movie_timescale, track)?);
    }
    let mut mehd = Mehd::default();
    if fragment_duration > u64::from(u32::MAX) {
        mehd.set_version(1);
        mehd.fragment_duration_v1 = fragment_duration;
    } else {
        mehd.fragment_duration_v0 = u32::try_from(fragment_duration)
            .map_err(|_| MuxError::LayoutOverflow("fragmented mehd duration"))?;
    }
    children.push(encode_typed_box(&mehd, &[])?);
    for track in tracks {
        let mut trex = Trex::default();
        trex.track_id = track.config.track_id();
        trex.default_sample_description_index = track
            .samples
            .first()
            .map(|sample| sample.sample_description_index)
            .unwrap_or(1);
        trex.default_sample_duration = track
            .samples
            .first()
            .map(|sample| sample.duration_media)
            .unwrap_or(0);
        trex.default_sample_size = 0;
        trex.default_sample_flags = 0;
        children.push(encode_typed_box(&trex, &[])?);
    }
    encode_typed_box(&Mvex, &children.concat())
}

fn build_fragment_moof_bytes(
    runs: &[FragmentTrackRun<'_>],
    mdat_header_size: usize,
    sequence_number: u32,
) -> Result<Vec<u8>, MuxError> {
    let mut mfhd = Mfhd::default();
    mfhd.sequence_number = sequence_number;
    let provisional_trafs = runs
        .iter()
        .map(|run| build_traf_bytes(run.track, &run.samples, 0))
        .collect::<Result<Vec<_>, _>>()?;
    let provisional_moof = encode_typed_box(
        &Moof,
        &[encode_typed_box(&mfhd, &[])?, provisional_trafs.concat()].concat(),
    )?;
    let moof_and_mdat_header_size = provisional_moof
        .len()
        .checked_add(mdat_header_size)
        .ok_or(MuxError::LayoutOverflow("fragment data offset"))?;
    let trafs = runs
        .iter()
        .map(|run| {
            let data_offset = u64::try_from(moof_and_mdat_header_size)
                .map_err(|_| MuxError::LayoutOverflow("fragment data offset"))?
                .checked_add(run.payload_offset)
                .ok_or(MuxError::LayoutOverflow("fragment data offset"))?;
            build_traf_bytes(
                run.track,
                &run.samples,
                i32::try_from(data_offset)
                    .map_err(|_| MuxError::LayoutOverflow("fragment data offset"))?,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let moof = encode_typed_box(
        &Moof,
        &[encode_typed_box(&mfhd, &[])?, trafs.concat()].concat(),
    )?;
    if moof.len() != provisional_moof.len() {
        return Err(MuxError::LayoutOverflow(
            "fragment moof size changed after data-offset resolution",
        ));
    }
    Ok(moof)
}

fn build_traf_bytes(
    track: &PreparedTrack<'_>,
    samples: &[PreparedSample],
    data_offset: i32,
) -> Result<Vec<u8>, MuxError> {
    let mut tfhd = Tfhd::default();
    tfhd.track_id = track.config.track_id();
    tfhd.sample_description_index = common_sample_description_index(track, samples)?;
    tfhd.set_flags(TFHD_DEFAULT_BASE_IS_MOOF | TFHD_SAMPLE_DESCRIPTION_INDEX_PRESENT);

    if let Some(default_duration) =
        all_equal_u32(samples.iter().map(|sample| sample.duration_media))
    {
        tfhd.set_flags(tfhd.flags() | TFHD_DEFAULT_SAMPLE_DURATION_PRESENT);
        tfhd.default_sample_duration = default_duration;
    }
    if let Some(default_size) = all_equal_u32(
        samples
            .iter()
            .map(|sample| u32::try_from(sample.sample_size).unwrap_or(u32::MAX)),
    ) {
        tfhd.set_flags(tfhd.flags() | TFHD_DEFAULT_SAMPLE_SIZE_PRESENT);
        tfhd.default_sample_size = default_size;
    }
    let force_first_only_flags = matches!(
        track.config.sync_sample_table_mode,
        super::SyncSampleTableMode::ForceFirstOnly
    );
    let first_sync_sample_index = force_first_only_flags
        .then(|| samples.iter().position(|sample| sample.is_sync_sample))
        .flatten();
    if let Some(default_flags) =
        all_equal_u32(samples.iter().enumerate().map(|(sample_index, sample)| {
            sample_flags(sample, sample_index, first_sync_sample_index)
        }))
    {
        tfhd.set_flags(tfhd.flags() | TFHD_DEFAULT_SAMPLE_FLAGS_PRESENT);
        tfhd.default_sample_flags = default_flags;
    }

    let mut tfdt = Tfdt::default();
    let base_decode_time = samples
        .first()
        .map(|sample| sample.decode_time_media)
        .unwrap_or(0);
    if base_decode_time > u64::from(u32::MAX) {
        tfdt.set_version(1);
        tfdt.base_media_decode_time_v1 = base_decode_time;
    } else {
        tfdt.base_media_decode_time_v0 = u32::try_from(base_decode_time)
            .map_err(|_| MuxError::LayoutOverflow("tfdt decode time"))?;
    }

    let trun = build_trun(track, samples, data_offset)?;
    let mut children = vec![
        encode_typed_box(&tfhd, &[])?,
        encode_typed_box(&tfdt, &[])?,
        encode_typed_box(&trun, &[])?,
    ];
    if fragmented_track_emits_roll_assignment(track) {
        children.push(encode_typed_box(
            &build_roll_sbgp(
                u32::try_from(samples.len())
                    .map_err(|_| MuxError::LayoutOverflow("fragment roll sample count"))?,
            ),
            &[],
        )?);
    }
    encode_typed_box(&Traf, &children.concat())
}

fn common_sample_description_index(
    track: &PreparedTrack<'_>,
    samples: &[PreparedSample],
) -> Result<u32, MuxError> {
    let Some(first) = samples.first() else {
        return Ok(1);
    };
    if first.sample_description_index == 0 {
        return Err(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: format!(
                "track {} uses sample description index 0",
                track.config.track_id()
            ),
        });
    }
    let sample_entry_count = u32::try_from(track.config.sample_entry_boxes().len())
        .map_err(|_| MuxError::LayoutOverflow("stsd entry_count"))?;
    if first.sample_description_index > sample_entry_count {
        return Err(MuxError::InvalidOutputLayout {
            layout: "fragmented",
            message: format!(
                "track {} uses sample description index {} with only {} sample entries",
                track.config.track_id(),
                first.sample_description_index,
                sample_entry_count
            ),
        });
    }
    if samples
        .iter()
        .all(|sample| sample.sample_description_index == first.sample_description_index)
    {
        return Ok(first.sample_description_index);
    }
    Err(MuxError::InvalidOutputLayout {
        layout: "fragmented",
        message: format!(
            "track {} has mixed sample descriptions inside one fragment run",
            track.config.track_id()
        ),
    })
}

fn build_trun(
    track: &PreparedTrack<'_>,
    samples: &[PreparedSample],
    data_offset: i32,
) -> Result<Trun, MuxError> {
    let mut trun = Trun::default();
    trun.sample_count =
        u32::try_from(samples.len()).map_err(|_| MuxError::LayoutOverflow("trun sample count"))?;
    trun.data_offset = data_offset;
    trun.set_flags(TRUN_DATA_OFFSET_PRESENT);
    let first_sync_sample_index = matches!(
        track.config.sync_sample_table_mode,
        super::SyncSampleTableMode::ForceFirstOnly
    )
    .then(|| samples.iter().position(|sample| sample.is_sync_sample))
    .flatten();
    if !samples
        .iter()
        .all(|sample| sample.composition_offset_media == 0)
    {
        trun.set_flags(trun.flags() | TRUN_SAMPLE_COMPOSITION_TIME_OFFSET_PRESENT);
        if samples
            .iter()
            .any(|sample| sample.composition_offset_media < 0)
        {
            trun.set_version(1);
        }
    }
    if all_equal_u32(samples.iter().map(|sample| sample.duration_media)).is_none() {
        trun.set_flags(trun.flags() | TRUN_SAMPLE_DURATION_PRESENT);
    }
    if all_equal_u32(
        samples
            .iter()
            .map(|sample| u32::try_from(sample.sample_size).unwrap_or(u32::MAX)),
    )
    .is_none()
    {
        trun.set_flags(trun.flags() | TRUN_SAMPLE_SIZE_PRESENT);
    }
    if all_equal_u32(
        samples.iter().enumerate().map(|(sample_index, sample)| {
            sample_flags(sample, sample_index, first_sync_sample_index)
        }),
    )
    .is_none()
    {
        trun.set_flags(trun.flags() | TRUN_SAMPLE_FLAGS_PRESENT);
    }
    trun.entries = samples
        .iter()
        .enumerate()
        .map(|(sample_index, sample)| {
            Ok(TrunEntry {
                sample_duration: sample.duration_media,
                sample_size: u32::try_from(sample.sample_size)
                    .map_err(|_| MuxError::LayoutOverflow("trun sample size"))?,
                sample_flags: sample_flags(sample, sample_index, first_sync_sample_index),
                sample_composition_time_offset_v0: u32::try_from(sample.composition_offset_media)
                    .unwrap_or(0),
                sample_composition_time_offset_v1: sample.composition_offset_media,
            })
        })
        .collect::<Result<Vec<_>, MuxError>>()?;
    Ok(trun)
}

fn build_sidx_bytes(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    fragments: &[FragmentLayout],
    single_sidx_reference: bool,
) -> Result<Vec<u8>, MuxError> {
    if track_uses_direct_iamf_flat_timing(track)
        || (sample_entry_matches(track.sample_entry_box, &[b"iamf"])
            && track
                .samples
                .iter()
                .any(|sample| sample.duration_movie == u32::MAX))
    {
        return Ok(Vec::new());
    }
    let mut sidx = Sidx::default();
    sidx.reference_id = track.config.track_id();
    sidx.timescale = file_config.movie_timescale();
    let presentation_trim = sidx_presentation_trim(track, file_config)?;
    let built_references = if let Some(reference_group_fragment_counts) =
        track.fragmented_reference_group_fragment_counts.as_deref()
    {
        build_grouped_sidx_references(
            track,
            fragments,
            reference_group_fragment_counts,
            presentation_trim,
            true,
        )?
    } else if single_sidx_reference {
        vec![build_sidx_reference(
            fragments.iter(),
            presentation_trim,
            true,
        )?]
    } else {
        fragments
            .iter()
            .enumerate()
            .map(|(index, fragment)| {
                build_sidx_reference(
                    std::iter::once(fragment),
                    if index == 0 { presentation_trim } else { 0 },
                    true,
                )
            })
            .collect::<Result<Vec<_>, MuxError>>()?
    };
    let earliest_presentation_time = built_references
        .first()
        .map(|reference| reference.earliest_presentation_time)
        .unwrap_or(0);
    if earliest_presentation_time > u64::from(u32::MAX) {
        sidx.set_version(1);
        sidx.earliest_presentation_time_v1 = earliest_presentation_time;
        sidx.first_offset_v1 = 0;
    } else {
        sidx.earliest_presentation_time_v0 = u32::try_from(earliest_presentation_time)
            .map_err(|_| MuxError::LayoutOverflow("sidx earliest presentation time"))?;
        sidx.first_offset_v0 = 0;
    }
    let built_references = limit_sidx_references(built_references);
    sidx.references = built_references
        .into_iter()
        .map(|reference| reference.reference)
        .collect();
    sidx.reference_count = u16::try_from(sidx.references.len())
        .map_err(|_| MuxError::LayoutOverflow("sidx reference count"))?;
    encode_typed_box(&sidx, &[])
}

fn build_fragment_segment_index_bytes(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    fragments: &[FragmentLayout],
) -> Result<Vec<Vec<u8>>, MuxError> {
    if track_uses_direct_iamf_flat_timing(track)
        || (sample_entry_matches(track.sample_entry_box, &[b"iamf"])
            && track
                .samples
                .iter()
                .any(|sample| sample.duration_movie == u32::MAX))
    {
        return Ok(vec![Vec::new(); fragments.len()]);
    }

    let presentation_trim = sidx_presentation_trim(track, file_config)?;
    fragments
        .iter()
        .enumerate()
        .map(|(index, fragment)| {
            let built_reference = build_sidx_reference(
                std::iter::once(fragment),
                if index == 0 { presentation_trim } else { 0 },
                false,
            )?;
            let mut sidx = Sidx::default();
            sidx.reference_id = track.config.track_id();
            sidx.timescale = file_config.movie_timescale();
            if built_reference.earliest_presentation_time > u64::from(u32::MAX) {
                sidx.set_version(1);
                sidx.earliest_presentation_time_v1 = built_reference.earliest_presentation_time;
                sidx.first_offset_v1 = 0;
            } else {
                sidx.earliest_presentation_time_v0 =
                    u32::try_from(built_reference.earliest_presentation_time)
                        .map_err(|_| MuxError::LayoutOverflow("sidx earliest presentation time"))?;
                sidx.first_offset_v0 = 0;
            }
            sidx.references = vec![built_reference.reference];
            sidx.reference_count = 1;
            encode_typed_box(&sidx, &[])
        })
        .collect()
}

fn limit_sidx_references(mut references: Vec<BuiltSidxReference>) -> Vec<BuiltSidxReference> {
    if references.len() > MAX_SIDX_REFERENCES {
        references.truncate(MAX_SIDX_REFERENCES);
    }
    references
}

fn sidx_presentation_trim(
    track: &PreparedTrack<'_>,
    file_config: &MuxFileConfig,
) -> Result<u64, MuxError> {
    track
        .edit_media_time
        .map(|media_time| {
            scale_track_time_to_movie(
                track.config.track_id(),
                i64::try_from(media_time)
                    .map_err(|_| MuxError::LayoutOverflow("sidx edit-list trim"))?,
                track.config.timescale(),
                file_config.movie_timescale(),
            )
            .and_then(|value| {
                u64::try_from(value).map_err(|_| MuxError::LayoutOverflow("sidx edit-list trim"))
            })
        })
        .transpose()?
        .map_or(Ok(0), Ok)
}

fn build_grouped_sidx_references(
    track: &PreparedTrack<'_>,
    fragments: &[FragmentLayout],
    reference_group_fragment_counts: &[u32],
    presentation_trim: u64,
    include_segment_type: bool,
) -> Result<Vec<BuiltSidxReference>, MuxError> {
    let mut references = Vec::with_capacity(reference_group_fragment_counts.len());
    let mut fragment_index = 0_usize;
    for (group_index, &group_fragment_count) in reference_group_fragment_counts.iter().enumerate() {
        let fragment_count = usize::try_from(group_fragment_count)
            .map_err(|_| MuxError::LayoutOverflow("sidx grouped fragment count"))?;
        let next_fragment_index = fragment_index
            .checked_add(fragment_count)
            .ok_or(MuxError::LayoutOverflow("sidx grouped fragment indexing"))?;
        let fragment_group = fragments
            .get(fragment_index..next_fragment_index)
            .ok_or_else(|| MuxError::InvalidChunkPlan {
                track_id: track.config.track_id(),
                message: "fragment reference groups ran past the planned fragment count"
                    .to_string(),
            })?;
        references.push(build_sidx_reference(
            fragment_group.iter(),
            if group_index == 0 {
                presentation_trim
            } else {
                0
            },
            include_segment_type,
        )?);
        fragment_index = next_fragment_index;
    }
    if fragment_index != fragments.len() {
        return Err(MuxError::InvalidChunkPlan {
            track_id: track.config.track_id(),
            message: "fragment reference groups did not cover every planned fragment".to_string(),
        });
    }
    Ok(references)
}

fn build_sidx_reference<'a, I>(
    fragments: I,
    presentation_trim: u64,
    include_segment_type: bool,
) -> Result<BuiltSidxReference, MuxError>
where
    I: IntoIterator<Item = &'a FragmentLayout>,
{
    let mut referenced_size = 0_usize;
    let mut subsegment_duration = 0_u64;
    let mut starts_with_sap = false;
    let mut saw_any_sample = false;
    let mut earliest_presentation_time = None::<u64>;
    let mut first_sap_time = None::<u64>;
    let presentation_trim_i128 = i128::from(presentation_trim);

    for fragment in fragments {
        if !saw_any_sample && !fragment.sidx_samples.is_empty() {
            starts_with_sap = fragment
                .sidx_samples
                .first()
                .map(|sample| sample.is_sync_sample)
                .unwrap_or(false);
            saw_any_sample = true;
        }
        if include_segment_type {
            referenced_size = referenced_size
                .checked_add(fragment.segment_type_bytes.len())
                .ok_or(MuxError::LayoutOverflow("sidx referenced size"))?;
        }
        referenced_size = fragment
            .metadata_bytes
            .iter()
            .try_fold(referenced_size, |total, bytes| {
                total.checked_add(bytes.len())
            })
            .and_then(|size| size.checked_add(fragment.moof_bytes.len()))
            .and_then(|size| size.checked_add(fragment.mdat_header.len()))
            .ok_or(MuxError::LayoutOverflow("sidx referenced size"))?;
        let payload_size = fragment.samples.iter().try_fold(0_usize, |total, sample| {
            total
                .checked_add(
                    usize::try_from(sample.sample_size)
                        .map_err(|_| MuxError::LayoutOverflow("sidx referenced size"))?,
                )
                .ok_or(MuxError::LayoutOverflow("sidx referenced size"))
        })?;
        referenced_size = referenced_size
            .checked_add(payload_size)
            .ok_or(MuxError::LayoutOverflow("sidx referenced size"))?;
        for sample in &fragment.sidx_samples {
            let presentation_start = i128::from(sample.decode_time_movie)
                .saturating_add(i128::from(sample.composition_offset_movie))
                .saturating_sub(presentation_trim_i128);
            let presentation_end =
                presentation_start.saturating_add(i128::from(sample.duration_movie));
            if presentation_start < 0 {
                if presentation_end > 0 {
                    let clipped_duration = u64::try_from(presentation_end)
                        .map_err(|_| MuxError::LayoutOverflow("sidx subsegment duration"))?;
                    subsegment_duration = subsegment_duration
                        .checked_add(clipped_duration)
                        .ok_or(MuxError::LayoutOverflow("sidx subsegment duration"))?;
                    earliest_presentation_time = Some(0);
                    if sample.is_sync_sample && first_sap_time.is_none() {
                        first_sap_time = Some(0);
                    }
                }
            } else {
                let normalized_presentation_start = u64::try_from(presentation_start)
                    .map_err(|_| MuxError::LayoutOverflow("sidx presentation start"))?;
                subsegment_duration = subsegment_duration
                    .checked_add(u64::from(sample.duration_movie))
                    .ok_or(MuxError::LayoutOverflow("sidx subsegment duration"))?;
                earliest_presentation_time = Some(
                    earliest_presentation_time.map_or(normalized_presentation_start, |current| {
                        current.min(normalized_presentation_start)
                    }),
                );
                if sample.is_sync_sample && first_sap_time.is_none() {
                    first_sap_time = Some(normalized_presentation_start);
                }
            }
        }
    }
    let earliest_presentation_time = earliest_presentation_time.unwrap_or(0);
    let sap_delta_time = first_sap_time
        .map(|first_sap_time| {
            u32::try_from(first_sap_time.saturating_sub(earliest_presentation_time))
                .map_err(|_| MuxError::LayoutOverflow("sidx SAP delta time"))
        })
        .transpose()?
        .unwrap_or(0);

    Ok(BuiltSidxReference {
        reference: SidxReference {
            reference_type: false,
            referenced_size: u32::try_from(referenced_size)
                .map_err(|_| MuxError::LayoutOverflow("sidx referenced size"))?,
            subsegment_duration: u32::try_from(subsegment_duration)
                .map_err(|_| MuxError::LayoutOverflow("sidx subsegment duration"))?,
            starts_with_sap,
            sap_type: if first_sap_time.is_some() { 1 } else { 0 },
            sap_delta_time,
        },
        earliest_presentation_time,
    })
}

fn build_ftyp_bytes(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
) -> Result<Vec<u8>, MuxError> {
    let (major_brand, minor_version, compatible_brands) =
        if file_config.auto_flat_profile() && !file_config.keep_flat_authority_brands() {
            infer_auto_flat_ftyp_profile(tracks)
        } else {
            (
                file_config.major_brand(),
                file_config.minor_version(),
                file_config.compatible_brands().to_vec(),
            )
        };
    let ftyp = Ftyp {
        major_brand,
        minor_version,
        compatible_brands,
    };
    encode_typed_box(&ftyp, &[])
}

fn infer_auto_flat_ftyp_profile(tracks: &[PreparedTrack<'_>]) -> (FourCc, u32, Vec<FourCc>) {
    let imported_authority_tracks = tracks.iter().all(track_uses_imported_authority_headers);
    let has_iamf = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"iamf"]));
    let has_qcp = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"sqcp", b"sevc", b"ssmv"]));
    let has_av1 = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"av01"]));
    let has_hevc = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[b"hvc1", b"hev1", b"dvh1", b"dvhe"],
        )
    });
    let has_prores = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[b"apco", b"apcn", b"apch", b"apcs", b"ap4x", b"ap4h"],
        )
    });
    let has_avs3 = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"avs3"]));
    let has_vvc = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"vvc1", b"vvi1"]));
    let has_avc = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[b"avc1", b"avc2", b"avc3", b"avc4"],
        )
    });
    let has_h263 = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"s263"]));

    if has_iamf {
        if imported_authority_tracks {
            let mut brands = vec![FourCc::from_bytes(*b"isom")];
            if has_avc {
                brands.push(FourCc::from_bytes(*b"avc1"));
            }
            if has_av1 {
                brands.push(FourCc::from_bytes(*b"av01"));
            }
            brands.push(FourCc::from_bytes(*b"iamf"));
            return (FourCc::from_bytes(*b"isom"), 1, brands);
        }
        return (
            FourCc::from_bytes(*b"mp42"),
            0,
            vec![
                FourCc::from_bytes(*b"isom"),
                FourCc::from_bytes(*b"mp42"),
                FourCc::from_bytes(*b"iso6"),
                FourCc::from_bytes(*b"iamf"),
            ],
        );
    }
    if has_qcp {
        let mut brands = vec![FourCc::from_bytes(*b"isom")];
        if has_avc {
            brands.push(FourCc::from_bytes(*b"avc1"));
        }
        brands.push(FourCc::from_bytes(*b"3g2a"));
        return (FourCc::from_bytes(*b"3g2a"), 65_536, brands);
    }
    if has_prores {
        return (
            FourCc::from_bytes(*b"qt  "),
            0x200,
            vec![FourCc::from_bytes(*b"qt  ")],
        );
    }
    if has_av1 {
        let mut brands = Vec::new();
        if has_avc {
            brands.push(FourCc::from_bytes(*b"avc1"));
        }
        brands.push(FourCc::from_bytes(*b"iso4"));
        brands.push(FourCc::from_bytes(*b"av01"));
        return (FourCc::from_bytes(*b"iso4"), 1, brands);
    }
    if has_hevc {
        let mut brands = Vec::new();
        if has_avc {
            brands.push(FourCc::from_bytes(*b"avc1"));
        }
        brands.push(FourCc::from_bytes(*b"iso4"));
        return (FourCc::from_bytes(*b"iso4"), 1, brands);
    }
    if has_avs3 {
        let mut brands = Vec::new();
        if has_avc {
            brands.push(FourCc::from_bytes(*b"avc1"));
        }
        brands.push(FourCc::from_bytes(*b"iso4"));
        brands.push(FourCc::from_bytes(*b"cav3"));
        return (FourCc::from_bytes(*b"iso4"), 1, brands);
    }
    if has_vvc {
        let mut brands = Vec::new();
        if has_avc {
            brands.push(FourCc::from_bytes(*b"avc1"));
        }
        brands.push(FourCc::from_bytes(*b"iso4"));
        return (FourCc::from_bytes(*b"iso4"), 1, brands);
    }
    if has_h263 {
        return (
            FourCc::from_bytes(*b"isom"),
            1,
            vec![
                FourCc::from_bytes(*b"isom"),
                FourCc::from_bytes(*b"3gg6"),
                FourCc::from_bytes(*b"3gg5"),
            ],
        );
    }
    if has_avc {
        return (
            FourCc::from_bytes(*b"isom"),
            1,
            vec![FourCc::from_bytes(*b"isom"), FourCc::from_bytes(*b"avc1")],
        );
    }
    (
        FourCc::from_bytes(*b"isom"),
        1,
        vec![FourCc::from_bytes(*b"isom")],
    )
}

fn sample_entry_matches(sample_entry_box: &[u8], names: &[&[u8; 4]]) -> bool {
    sample_entry_box
        .get(4..8)
        .map(|box_type| {
            names
                .iter()
                .any(|candidate| box_type == candidate.as_slice())
        })
        .unwrap_or(false)
}

fn sample_entry_esds_oti_matches(
    sample_entry_box: &[u8],
    sample_entry_types: &[&[u8; 4]],
    object_type_indication: u8,
) -> Result<bool, MuxError> {
    if !sample_entry_matches(sample_entry_box, sample_entry_types) {
        return Ok(false);
    }
    let sample_entry_type = sample_entry_box_type(sample_entry_box)?;
    let child_boxes = match sample_entry_type {
        value if value == FourCc::from_bytes(*b"mp4a") => {
            decode_audio_sample_entry_parts(sample_entry_box)?.1
        }
        value if value == FourCc::from_bytes(*b"mp4v") => {
            decode_visual_sample_entry_parts(sample_entry_box)?.1
        }
        _ => return Ok(false),
    };
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        for descriptor in esds.descriptors {
            if let Some(decoder_config) = descriptor.decoder_config_descriptor
                && decoder_config.object_type_indication == object_type_indication
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn mp4a_sample_entry_oti_matches(
    sample_entry_box: &[u8],
    object_type_indication: u8,
) -> Result<bool, MuxError> {
    sample_entry_esds_oti_matches(sample_entry_box, &[b"mp4a"], object_type_indication)
}

fn sample_entry_mp4a_object_type_indication(
    sample_entry_box: &[u8],
) -> Result<Option<u8>, MuxError> {
    if !sample_entry_matches(sample_entry_box, &[b"mp4a"]) {
        return Ok(None);
    }
    let child_boxes = decode_audio_sample_entry_parts(sample_entry_box)?.1;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        for descriptor in esds.descriptors {
            if let Some(decoder_config) = descriptor.decoder_config_descriptor {
                return Ok(Some(decoder_config.object_type_indication));
            }
        }
    }
    Ok(None)
}

fn mp4a_sample_entry_audio_profile_level_indication(
    sample_entry_box: &[u8],
) -> Result<Option<u8>, MuxError> {
    if !sample_entry_matches(sample_entry_box, &[b"mp4a"]) {
        return Ok(None);
    }
    let (sample_entry, child_boxes, _) = decode_audio_sample_entry_parts(sample_entry_box)?;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        if let Some(profile) =
            detect_aac_profile(&esds).map_err(|_| MuxError::LayoutOverflow("mp4a esds decode"))?
        {
            let sample_rate = sample_entry.sample_rate >> 16;
            return Ok(Some(match profile.audio_object_type {
                42 => 0x0f,
                29 => 0x2c,
                5 => {
                    if sample_rate <= 24_000 {
                        0x28
                    } else {
                        0x2c
                    }
                }
                2 if (sample_entry.sample_rate >> 16) == 24_000 => 0x28,
                _ => 0x29,
            }));
        }
    }
    Ok(None)
}

fn ec3_sample_entry_data_rate(sample_entry_box: &[u8]) -> Result<Option<u16>, MuxError> {
    if !sample_entry_matches(sample_entry_box, &[b"ec-3"]) {
        return Ok(None);
    }
    let Ok((_, child_boxes, _)) = decode_audio_sample_entry_parts(sample_entry_box) else {
        return Ok(None);
    };
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"dec3") {
            continue;
        }
        return Ok(Some(decode_typed_box::<Dec3>(&child_box)?.data_rate));
    }
    Ok(None)
}

fn fragmented_audio_average_bitrate(track: &PreparedTrack<'_>) -> Option<u64> {
    let summed_duration = track.samples.iter().try_fold(0_u64, |duration, sample| {
        duration.checked_add(u64::from(sample.duration_movie))
    })?;
    if summed_duration == 0 {
        return None;
    }
    let total_sample_size = track
        .samples
        .iter()
        .try_fold(0_u64, |size, sample| size.checked_add(sample.sample_size))?;
    total_sample_size
        .checked_mul(8)?
        .checked_mul(u64::from(track.config.timescale()))?
        .checked_div(summed_duration)
}

fn fragmented_ec3_mehd_trims_one_tick(
    sample_entry_box: &[u8],
    sample_rate: u32,
    sample_count: usize,
) -> Result<bool, MuxError> {
    if sample_rate != 48_000 {
        return Ok(false);
    }
    if sample_count.is_multiple_of(2) {
        return Ok(false);
    }
    Ok(ec3_sample_entry_data_rate(sample_entry_box)? != Some(640))
}

fn fragmented_mp4a_mehd_trims_one_tick(track: &PreparedTrack<'_>) -> Result<bool, MuxError> {
    if !track.samples.len().is_multiple_of(2)
        || track.samples.last().map(|sample| sample.duration_movie) != Some(1_024)
    {
        return Ok(false);
    }
    let average_bitrate = fragmented_audio_average_bitrate(track);
    if track_uses_imported_authority_headers(track)
        && average_bitrate.is_some_and(|bitrate| bitrate > 0 && bitrate <= 8_000)
    {
        return Ok(false);
    }
    if sample_entry_audio_sample_rate_int(track.sample_entry_box) == Some(44_100)
        && mp4a_sample_entry_audio_profile_level_indication(track.sample_entry_box)? == Some(0x29)
        && average_bitrate.is_some_and(|bitrate| (170_000..=210_000).contains(&bitrate))
    {
        return Ok(false);
    }
    Ok(true)
}

fn sample_entry_mp4v_object_type_indication(
    sample_entry_box: &[u8],
) -> Result<Option<u8>, MuxError> {
    if !sample_entry_matches(sample_entry_box, &[b"mp4v"]) {
        return Ok(None);
    }
    let child_boxes = decode_visual_sample_entry_parts(sample_entry_box)?.1;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        for descriptor in esds.descriptors {
            if let Some(decoder_config) = descriptor.decoder_config_descriptor {
                return Ok(Some(decoder_config.object_type_indication));
            }
        }
    }
    Ok(None)
}

fn prepare_tracks<'a>(
    file_config: &MuxFileConfig,
    track_configs: &'a [MuxTrackConfig],
    plan: &'a MuxPlan,
    use_override_decode_times: bool,
) -> Result<Vec<PreparedTrack<'a>>, MuxError> {
    let mut config_by_track_id = BTreeMap::<u32, &'a MuxTrackConfig>::new();
    for config in track_configs {
        if config.timescale() == 0 {
            return Err(MuxError::InvalidTrackTimescale {
                track_id: config.track_id(),
            });
        }
        validate_language(config)?;
        validate_sample_entry_box(config)?;
        match config_by_track_id.entry(config.track_id()) {
            Entry::Vacant(slot) => {
                slot.insert(config);
            }
            Entry::Occupied(_) => {
                return Err(MuxError::DuplicateTrackId {
                    track_id: config.track_id(),
                });
            }
        }
    }

    let mut samples_by_track = BTreeMap::<u32, Vec<&super::MuxPlannedMediaItem>>::new();
    for item in plan.planned_items() {
        samples_by_track
            .entry(item.staged().track_id())
            .or_default()
            .push(item);
    }

    for track_id in samples_by_track.keys().copied() {
        if !config_by_track_id.contains_key(&track_id) {
            return Err(MuxError::MissingTrackId { track_id });
        }
    }

    let mut prepared_tracks = Vec::with_capacity(track_configs.len());
    for config in track_configs {
        let samples = samples_by_track
            .remove(&config.track_id())
            .unwrap_or_default();
        prepared_tracks.push(prepare_track(
            file_config,
            plan,
            config,
            samples,
            use_override_decode_times,
        )?);
    }

    Ok(prepared_tracks)
}

fn prepare_track<'a>(
    file_config: &MuxFileConfig,
    plan: &'a MuxPlan,
    config: &'a MuxTrackConfig,
    samples: Vec<&'a super::MuxPlannedMediaItem>,
    use_override_decode_times: bool,
) -> Result<PreparedTrack<'a>, MuxError> {
    let mut previous_decode_time = None::<u64>;
    let mut prepared_samples = Vec::with_capacity(samples.len());
    let mut max_decode_end_media = 0_u64;
    let mut max_presentation_end_media = 0_u64;
    let timing_override = config.flat_timing_override();
    if let Some(override_value) = timing_override {
        if override_value.sample_durations.len() != samples.len() {
            return Err(MuxError::InvalidOutputLayout {
                layout: "flat",
                message: format!(
                    "track {} authored a timing override with {} sample durations for {} samples",
                    config.track_id(),
                    override_value.sample_durations.len(),
                    samples.len(),
                ),
            });
        }
        if override_value.composition_offsets.len() != samples.len() {
            return Err(MuxError::InvalidOutputLayout {
                layout: "flat",
                message: format!(
                    "track {} authored a timing override with {} composition offsets for {} samples",
                    config.track_id(),
                    override_value.composition_offsets.len(),
                    samples.len(),
                ),
            });
        }
    }
    let mut overridden_decode_time_media = config.fragmented_decode_time_offset().unwrap_or(0);

    for (sample_index, sample) in samples.into_iter().enumerate() {
        let staged = sample.staged();
        let sample_description_index = staged.sample_description_index();
        let sample_entry_count = u32::try_from(config.sample_entry_boxes().len())
            .map_err(|_| MuxError::LayoutOverflow("stsd entry_count"))?;
        if sample_description_index == 0 || sample_description_index > sample_entry_count {
            return Err(MuxError::InvalidOutputLayout {
                layout: "mp4",
                message: format!(
                    "track {} uses sample description index {} with {} sample entries",
                    config.track_id(),
                    sample_description_index,
                    sample_entry_count
                ),
            });
        }
        if let Some(previous_decode_time) = previous_decode_time
            && staged.decode_time() < previous_decode_time
        {
            return Err(MuxError::NonMonotonicTrackDecodeTime {
                track_id: config.track_id(),
                previous_decode_time,
                next_decode_time: staged.decode_time(),
            });
        }
        previous_decode_time = Some(staged.decode_time());

        let (
            duration_media,
            composition_offset_media,
            decode_time_media,
            _decode_end_media,
            duration_decode_end_media,
            duration_presentation_end_media,
        ) = if let Some(override_value) = timing_override {
            let duration_media = u64::from(override_value.sample_durations[sample_index]);
            let composition_offset_media = override_value.composition_offsets[sample_index];
            let duration_decode_time_media = overridden_decode_time_media;
            let duration_decode_end_media = duration_decode_time_media
                .checked_add(duration_media)
                .ok_or(MuxError::LayoutOverflow("track decode end"))?;
            overridden_decode_time_media = duration_decode_end_media;
            let decode_time_media = if use_override_decode_times {
                duration_decode_time_media
            } else {
                scale_movie_time_to_track(
                    config.track_id(),
                    staged.decode_time(),
                    file_config.movie_timescale(),
                    config.timescale(),
                )?
            };
            let decode_end_media = decode_time_media
                .checked_add(duration_media)
                .ok_or(MuxError::LayoutOverflow("track decode end"))?;
            let duration_presentation_end_media = i128::from(duration_decode_time_media)
                .saturating_add(i128::from(composition_offset_media))
                .saturating_add(i128::from(duration_media));
            (
                duration_media,
                composition_offset_media,
                decode_time_media,
                decode_end_media,
                duration_decode_end_media,
                duration_presentation_end_media,
            )
        } else {
            let duration_media = scale_movie_time_to_track(
                config.track_id(),
                u64::from(staged.duration()),
                file_config.movie_timescale(),
                config.timescale(),
            )?;
            let composition_offset_media = scale_movie_offset_to_track(
                config.track_id(),
                i64::from(staged.composition_time_offset()),
                file_config.movie_timescale(),
                config.timescale(),
            )?;
            let decode_time_media = scale_movie_time_to_track(
                config.track_id(),
                staged.decode_time(),
                file_config.movie_timescale(),
                config.timescale(),
            )?;
            let decode_end_movie = staged
                .decode_time()
                .checked_add(u64::from(staged.duration()))
                .ok_or(MuxError::LayoutOverflow("track decode end"))?;
            let decode_end_media = scale_movie_time_to_track(
                config.track_id(),
                decode_end_movie,
                file_config.movie_timescale(),
                config.timescale(),
            )?;
            (
                duration_media,
                composition_offset_media,
                decode_time_media,
                decode_end_media,
                decode_end_media,
                i128::from(decode_time_media)
                    .saturating_add(i128::from(composition_offset_media))
                    .saturating_add(i128::from(duration_media)),
            )
        };
        max_decode_end_media = max_decode_end_media.max(duration_decode_end_media);
        if duration_presentation_end_media > 0 {
            max_presentation_end_media = max_presentation_end_media.max(
                u64::try_from(duration_presentation_end_media)
                    .map_err(|_| MuxError::LayoutOverflow("presentation end time"))?,
            );
        }
        prepared_samples.push(PreparedSample {
            source_index: staged.source_index(),
            source_data_offset: staged.data_offset(),
            decode_time_movie: staged.decode_time(),
            decode_time_media,
            output_offset: sample.output_offset(),
            sample_size: u64::from(staged.data_size()),
            duration_movie: staged.duration(),
            duration_media: u32::try_from(duration_media)
                .map_err(|_| MuxError::LayoutOverflow("sample duration"))?,
            composition_offset_movie: staged.composition_time_offset(),
            composition_offset_media,
            is_sync_sample: staged.is_sync_sample(),
            sample_description_index,
        });
    }

    let media_duration = max_decode_end_media
        .max(max_presentation_end_media)
        .saturating_sub(config.fragmented_decode_time_offset().unwrap_or(0));
    let presentation_duration_media = config
        .edit_media_time()
        .map_or(media_duration, |edit_media_time| {
            media_duration.saturating_sub(edit_media_time)
        });
    Ok(PreparedTrack {
        config,
        sample_entry_box: config.sample_entry_box(),
        samples: prepared_samples,
        chunk_sample_counts: if previous_decode_time.is_some() {
            plan.chunk_sample_counts(config.track_id())?.to_vec()
        } else {
            Vec::new()
        },
        fragmented_reference_group_fragment_counts: config
            .fragmented_reference_group_fragment_counts()
            .map(|counts| counts.to_vec()),
        media_duration,
        presentation_duration_media,
        edit_media_time: config.edit_media_time(),
        flat_timing_override: timing_override,
    })
}

fn build_moov_bytes(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
    ftyp_size: u64,
    mdat_header_size: u64,
    mdat_data_start: u64,
) -> Result<Vec<u8>, MuxError> {
    let auto_flat_creation_time = auto_flat_creation_time(file_config)?;
    let mvhd = build_mvhd(file_config, tracks, auto_flat_creation_time)?;
    let mut children = Vec::new();
    children.extend_from_slice(&encode_typed_box(&mvhd, &[])?);
    if let Some(iods_bytes) = build_flat_iods_bytes(file_config, tracks)? {
        children.extend_from_slice(&iods_bytes);
    }
    for track in tracks {
        children.extend_from_slice(&build_trak_bytes(
            file_config,
            track,
            ftyp_size,
            mdat_header_size,
            mdat_data_start,
            auto_flat_creation_time,
        )?);
    }
    if let Some(udta_bytes) = build_flat_udta_bytes(file_config, tracks)? {
        children.extend_from_slice(&udta_bytes);
    }
    encode_typed_box(&Moov, &children)
}

fn build_flat_iods_bytes(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
) -> Result<Option<Vec<u8>>, MuxError> {
    if let Some(preserved_iods_bytes) = file_config.preserved_flat_iods_bytes() {
        return Ok(Some(preserved_iods_bytes.to_vec()));
    }
    if !file_config.auto_flat_profile() {
        return Ok(None);
    }

    let has_audio = tracks.iter().any(|track| track.config.kind().is_audio());
    let has_mp4a = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mp4a"]));
    let has_vorbis_mp4a = tracks
        .iter()
        .any(|track| mp4a_sample_entry_oti_matches(track.sample_entry_box, 0xDD).unwrap_or(false));
    let has_voice_mp4a = tracks
        .iter()
        .any(|track| mp4a_sample_entry_oti_matches(track.sample_entry_box, 0xE1).unwrap_or(false));
    let first_mp4a_oti = tracks
        .iter()
        .find_map(|track| {
            sample_entry_mp4a_object_type_indication(track.sample_entry_box).transpose()
        })
        .transpose()?;
    let first_mp4a_audio_profile_level_indication = tracks
        .iter()
        .find_map(|track| {
            mp4a_sample_entry_audio_profile_level_indication(track.sample_entry_box).transpose()
        })
        .transpose()?;
    let first_flat_audio_profile_level_indication = tracks
        .iter()
        .find_map(|track| track.config.flat_audio_profile_level_indication());
    let has_opus = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"Opus"]));
    let has_speex = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"spex"]));
    let has_voice_3gpp_audio = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[b"samr", b"sawb", b"sqcp", b"sevc", b"ssmv"],
        )
    });
    let has_visual_track = tracks.iter().any(|track| track.config.kind().is_video());
    let has_timed_text_sample_entry = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"tx3g", b"wvtt", b"stpp"]));
    let has_mhm = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mhm1", b"mhm2"]));
    let has_iamf = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"iamf"]));
    let has_dts = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[
                b"dtsc", b"dtse", b"dtsh", b"dtsl", b"dtsm", b"dts-", b"dtsx", b"dtsy",
            ],
        )
    });
    let has_mp4s = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mp4s"]));
    let has_mp4v = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mp4v"]));
    let first_mp4v_oti = tracks
        .iter()
        .find_map(|track| {
            sample_entry_mp4v_object_type_indication(track.sample_entry_box).transpose()
        })
        .transpose()?;
    let first_mp4v_profile_level = tracks
        .iter()
        .find_map(|track| {
            sample_entry_mp4v_visual_profile_level(track.sample_entry_box).transpose()
        })
        .transpose()?;
    let has_mpeg2_mp4v = matches!(first_mp4v_oti, Some(0x60..=0x65));
    let has_theora_mp4v = tracks.iter().any(|track| {
        sample_entry_esds_oti_matches(track.sample_entry_box, &[b"mp4v"], 0xDF).unwrap_or(false)
    });
    let has_other_iods_codec = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mp4v", b"mp4s", b"spex"]));
    let has_non_mp4a_audio = has_audio
        && tracks.iter().any(|track| {
            track.config.kind().is_audio()
                && !sample_entry_matches(track.sample_entry_box, &[b"mp4a"])
        });
    let imported_authority_tracks = tracks.iter().all(track_uses_imported_authority_headers);
    let has_avc = tracks.iter().any(|track| {
        sample_entry_matches(
            track.sample_entry_box,
            &[b"avc1", b"avc2", b"avc3", b"avc4"],
        )
    });
    let has_vvc = tracks
        .iter()
        .any(|track| sample_entry_matches(track.sample_entry_box, &[b"vvc1", b"vvi1"]));
    let has_imported_authority_mhm1_only = imported_authority_tracks
        && has_mhm
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf;
    let has_imported_authority_mha1_only = imported_authority_tracks
        && tracks
            .iter()
            .any(|track| sample_entry_matches(track.sample_entry_box, &[b"mha1", b"mha2"]))
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf;
    let has_imported_authority_opus_only = imported_authority_tracks
        && has_opus
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf;
    let has_imported_authority_vorbis_only = imported_authority_tracks
        && has_vorbis_mp4a
        && !has_avc
        && !has_vvc
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf
        && !has_mhm
        && !has_opus;
    let has_imported_authority_voice_mp4a_only = imported_authority_tracks
        && has_voice_mp4a
        && !has_avc
        && !has_vvc
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf
        && !has_mhm
        && !has_opus;
    let has_imported_authority_direct_voice_only = imported_authority_tracks
        && has_voice_3gpp_audio
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
        && !has_iamf
        && !has_mhm
        && !has_opus;
    let has_flat_iods_omitted_speex_only = tracks.iter().any(|track| track.config.omit_flat_iods())
        && has_speex
        && !has_visual_track
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_mp4v
        && !has_mp4s
        && !has_iamf
        && !has_mhm
        && !has_opus;
    let has_transport_clocked_mhm1 = tracks.iter().any(|track| {
        sample_entry_matches(track.sample_entry_box, &[b"mhm1", b"mhm2"])
            && sample_entry_audio_sample_rate_int(track.sample_entry_box)
                .is_some_and(|sample_rate| sample_rate != track.config.timescale())
    });
    let has_direct_vvc_only = !imported_authority_tracks
        && has_vvc
        && !has_audio
        && !has_avc
        && !has_other_iods_codec
        && !has_mp4s
        && !has_mhm
        && !has_iamf;
    if has_imported_authority_mhm1_only {
        return Ok(None);
    }
    if has_imported_authority_mha1_only {
        return Ok(None);
    }
    if has_imported_authority_opus_only {
        return Ok(None);
    }
    if has_imported_authority_vorbis_only {
        return Ok(None);
    }
    if has_imported_authority_voice_mp4a_only {
        return Ok(None);
    }
    if has_imported_authority_direct_voice_only {
        return Ok(None);
    }
    if has_flat_iods_omitted_speex_only {
        return Ok(None);
    }
    if has_iamf
        && !imported_authority_tracks
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
        && !has_mhm
    {
        return Ok(None);
    }
    if has_transport_clocked_mhm1
        && !has_avc
        && !has_vvc
        && !has_mp4a
        && !has_other_iods_codec
        && !has_mp4s
    {
        return Ok(None);
    }
    if has_direct_vvc_only {
        return Ok(None);
    }
    if !(has_mp4a
        || has_avc
        || has_vvc
        || has_opus
        || has_other_iods_codec
        || has_mp4s
        || has_mhm
        || has_iamf
        || (has_audio && file_config.allow_audio_only_iods()))
    {
        return Ok(None);
    }

    let descriptor = Descriptor::from_initial_object_descriptor(InitialObjectDescriptor {
        object_descriptor_id: 1,
        include_inline_profile_level_flag: false,
        od_profile_level_indication: 0xff,
        scene_profile_level_indication: 0xff,
        audio_profile_level_indication: if has_mhm && has_avc {
            0xfe
        } else if has_mhm {
            first_flat_audio_profile_level_indication.unwrap_or(0x0c)
        } else if has_vorbis_mp4a {
            0x10
        } else if has_mp4a {
            if first_mp4a_oti == Some(0x40) {
                first_mp4a_audio_profile_level_indication.unwrap_or(0x29)
            } else {
                0xfe
            }
        } else if has_iamf
            || ((has_dts || has_audio) && !has_avc && file_config.allow_audio_only_iods())
            || (has_voice_3gpp_audio && has_visual_track)
            || has_opus
            || (has_speex && !has_visual_track)
        {
            0xfe
        } else {
            0xff
        },
        visual_profile_level_indication: if has_mpeg2_mp4v {
            if imported_authority_tracks {
                0xfe
            } else {
                0x0c
            }
        } else if let Some(profile_level_indication) = first_mp4v_profile_level {
            profile_level_indication
        } else if first_mp4v_oti == Some(0x6a) {
            0x6a
        } else if has_theora_mp4v {
            0xfe
        } else if has_mp4v {
            0x01
        } else if has_vvc && !has_avc && has_mp4a {
            0xff
        } else if (has_vvc && !has_avc) || (has_avc && !has_audio && imported_authority_tracks) {
            0xfe
        } else if has_avc && has_mp4a && imported_authority_tracks {
            0xff
        } else if has_avc && has_mp4a {
            0x7f
        } else if has_avc && (has_non_mp4a_audio || has_timed_text_sample_entry) {
            0x15
        } else if has_avc {
            0x7f
        } else {
            0xff
        },
        graphics_profile_level_indication: 0xff,
        ..InitialObjectDescriptor::default()
    })
    .map_err(|error| MuxError::InvalidOutputLayout {
        layout: "flat",
        message: format!("failed to build iods descriptor: {error}"),
    })?;

    let mut iods = Iods::default();
    iods.descriptor = Some(descriptor);
    Ok(Some(encode_typed_box(&iods, &[])?))
}

fn sample_entry_mp4v_visual_profile_level(sample_entry_box: &[u8]) -> Result<Option<u8>, MuxError> {
    if !sample_entry_matches(sample_entry_box, &[b"mp4v"]) {
        return Ok(None);
    }
    let child_boxes = decode_visual_sample_entry_parts(sample_entry_box)?.1;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        if let Some(decoder_specific_info) = esds.decoder_specific_info() {
            return Ok(super::demux::mp4v_profile_level_indication(
                decoder_specific_info,
            ));
        }
    }
    Ok(None)
}

fn build_flat_udta_bytes(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
) -> Result<Option<Vec<u8>>, MuxError> {
    if let Some(preserved_udta_bytes) = file_config.preserved_flat_udta_bytes() {
        return Ok(Some(preserved_udta_bytes.to_vec()));
    }
    if !file_config.auto_flat_profile() {
        return Ok(None);
    }
    let tool_metadata = if let Some(encoding_metadata) = file_config.flat_source_encoding_metadata()
    {
        Some(encoding_metadata)
    } else if file_config.emit_default_flat_tool_metadata() {
        Some(FLAT_TOOL_METADATA_VALUE)
    } else {
        None
    };
    let encoder_metadata = file_config.flat_source_encoder_metadata();
    if tool_metadata.is_none() && encoder_metadata.is_none() {
        return Ok(None);
    }

    let mut metadata_handler = Hdlr::default();
    metadata_handler.handler_type = FourCc::from_bytes(*b"mdir");
    metadata_handler.name.clear();

    let mut ilst_children = Vec::new();
    if let Some(tool_metadata) = tool_metadata
        && !tool_metadata.is_empty()
    {
        let mut encoding_tool_item = IlstMetaContainer::default();
        encoding_tool_item.set_box_type(FourCc::from_bytes([0xA9, b't', b'o', b'o']));

        let encoding_tool_data = Data {
            data_type: DATA_TYPE_STRING_UTF8,
            data_lang: 0,
            data: tool_metadata.as_bytes().to_vec(),
        };
        let encoding_tool_data_bytes = encode_typed_box(&encoding_tool_data, &[])?;
        ilst_children.extend_from_slice(&encode_typed_box(
            &encoding_tool_item,
            &encoding_tool_data_bytes,
        )?);
    }
    if let Some(encoder_metadata) = encoder_metadata
        && !encoder_metadata.is_empty()
    {
        let mut encoder_item = IlstMetaContainer::default();
        encoder_item.set_box_type(FourCc::from_bytes([0xA9, b'e', b'n', b'c']));

        let encoder_data = Data {
            data_type: DATA_TYPE_STRING_UTF8,
            data_lang: 0,
            data: encoder_metadata.as_bytes().to_vec(),
        };
        let encoder_data_bytes = encode_typed_box(&encoder_data, &[])?;
        ilst_children.extend_from_slice(&encode_typed_box(&encoder_item, &encoder_data_bytes)?);
    }
    if ilst_children.is_empty() {
        return Ok(None);
    }

    let ilst_bytes = encode_typed_box(&Ilst, &ilst_children)?;
    let meta_children = [encode_typed_box(&metadata_handler, &[])?, ilst_bytes].concat();
    let meta_bytes = if uses_quicktime_flat_metadata_container(tracks) {
        encode_raw_box(FourCc::from_bytes(*b"meta"), &meta_children)?
    } else {
        encode_typed_box(&Meta::default(), &meta_children)?
    };
    Ok(Some(encode_typed_box(&Udta, &meta_bytes)?))
}

fn uses_quicktime_flat_metadata_container(tracks: &[PreparedTrack<'_>]) -> bool {
    infer_auto_flat_ftyp_profile(tracks).0 == FourCc::from_bytes(*b"qt  ")
}

fn build_free_padding_bytes(file_config: &MuxFileConfig) -> Result<Vec<u8>, MuxError> {
    let _ = file_config;
    encode_raw_box(
        FourCc::from_bytes(*b"free"),
        &[0_u8; DEFAULT_FREE_PADDING_SIZE],
    )
}

fn current_isom_time() -> Result<u32, MuxError> {
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MuxError::LayoutOverflow("current MP4 time"))?
        .as_secs();
    let isom_seconds = unix_seconds
        .checked_add(ISOM_UNIX_EPOCH_OFFSET)
        .ok_or(MuxError::LayoutOverflow("current MP4 time"))?;
    u32::try_from(isom_seconds).map_err(|_| MuxError::LayoutOverflow("current MP4 time"))
}

fn auto_flat_creation_time(file_config: &MuxFileConfig) -> Result<Option<u32>, MuxError> {
    if file_config.auto_flat_profile() {
        Ok(Some(current_isom_time()?))
    } else {
        Ok(None)
    }
}

fn build_mvhd(
    file_config: &MuxFileConfig,
    tracks: &[PreparedTrack<'_>],
    auto_flat_creation_time: Option<u32>,
) -> Result<Mvhd, MuxError> {
    let movie_timescale = flat_movie_header_timescale(file_config);
    let movie_duration = tracks
        .iter()
        .map(|track| flat_movie_duration(track, movie_timescale))
        .max()
        .unwrap_or(0);
    let next_track_id = tracks
        .iter()
        .map(|track| track.config.track_id())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or(MuxError::LayoutOverflow("next_track_id"))?;

    let mut mvhd = Mvhd::default();
    mvhd.timescale = movie_timescale;
    if movie_duration > u64::from(u32::MAX)
        && !tracks.iter().all(track_uses_direct_iamf_flat_timing)
    {
        mvhd.set_version(1);
        mvhd.duration_v1 = movie_duration;
    } else {
        mvhd.duration_v0 = movie_duration as u32;
    }
    mvhd.rate = 0x0001_0000;
    mvhd.volume = 0x0100;
    mvhd.matrix = IDENTITY_MATRIX;
    mvhd.next_track_id = next_track_id;
    if let Some(auto_flat_creation_time) = auto_flat_creation_time {
        let modification_time = file_config
            .flat_source_movie_modification_time()
            .unwrap_or(u64::from(auto_flat_creation_time));
        apply_flat_movie_header_times(
            &mut mvhd,
            file_config.flat_source_movie_creation_time(),
            modification_time,
        )?;
    }
    Ok(mvhd)
}

fn flat_movie_header_timescale(file_config: &MuxFileConfig) -> u32 {
    if file_config.auto_flat_profile() && !file_config.preserve_auto_flat_movie_timescale() {
        AUTO_FLAT_MOVIE_TIMESCALE
    } else {
        file_config.movie_timescale()
    }
}

fn flat_movie_duration(track: &PreparedTrack<'_>, movie_timescale: u32) -> u64 {
    let presentation_duration_media = track
        .flat_timing_override
        .map(|override_value| override_value.presentation_duration)
        .unwrap_or(track.presentation_duration_media);
    if movie_timescale == track.config.timescale() {
        return presentation_duration_media;
    }
    let scaled_duration = presentation_duration_media.saturating_mul(u64::from(movie_timescale));
    if track_uses_imported_authority_headers(track) {
        let divisor = u64::from(track.config.timescale());
        scaled_duration
            .saturating_add(divisor / 2)
            .checked_div(divisor)
            .unwrap_or(0)
    } else {
        scaled_duration / u64::from(track.config.timescale())
    }
}

fn build_trak_bytes(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    ftyp_size: u64,
    mdat_header_size: u64,
    mdat_data_start: u64,
    auto_flat_creation_time: Option<u32>,
) -> Result<Vec<u8>, MuxError> {
    let tkhd = build_tkhd(file_config, track, auto_flat_creation_time)?;
    let mdia = build_mdia_bytes(
        file_config,
        track,
        ftyp_size,
        mdat_header_size,
        mdat_data_start,
        auto_flat_creation_time,
    )?;
    let mut children = vec![encode_typed_box(&tkhd, &[])?];
    let mut preserved_edts = Vec::new();
    let mut preserved_tref = Vec::new();
    let mut preserved_other_boxes = Vec::new();
    for child_box in track.config.preserved_flat_trak_boxes().iter().cloned() {
        match encoded_box_type(&child_box).ok() {
            Some(value) if value == FourCc::from_bytes(*b"edts") => preserved_edts.push(child_box),
            Some(value) if value == FourCc::from_bytes(*b"tref") => preserved_tref.push(child_box),
            _ => preserved_other_boxes.push(child_box),
        }
    }
    if !preserved_edts.is_empty() {
        children.extend(preserved_edts);
    } else if let Some(edts) = build_edts_bytes(
        track,
        flat_movie_duration(track, flat_movie_header_timescale(file_config)),
    )? {
        children.push(edts);
    }
    children.extend(preserved_tref);
    children.push(mdia);
    children.extend(preserved_other_boxes);
    encode_typed_box(&Trak, &children.concat())
}

fn build_tkhd(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    auto_flat_creation_time: Option<u32>,
) -> Result<Tkhd, MuxError> {
    let mut tkhd =
        build_tkhd_with_movie_timescale(track, flat_movie_header_timescale(file_config))?;
    if let Some(auto_flat_creation_time) = auto_flat_creation_time {
        let modification_time = if track.config.kind() == MuxTrackKind::Video
            && track_uses_imported_authority_headers(track)
        {
            track
                .config
                .flat_source_track_modification_time()
                .filter(|modification_time| *modification_time != 0)
                .unwrap_or(u64::from(auto_flat_creation_time))
        } else {
            u64::from(auto_flat_creation_time)
        };
        apply_flat_track_header_times(
            &mut tkhd,
            track.config.flat_source_track_creation_time(),
            modification_time,
        )?;
    }
    Ok(tkhd)
}

fn build_tkhd_with_movie_timescale(
    track: &PreparedTrack<'_>,
    movie_timescale: u32,
) -> Result<Tkhd, MuxError> {
    let mut tkhd = Tkhd::default();
    tkhd.set_flags(track.config.tkhd_flags());
    tkhd.track_id = track.config.track_id();
    let movie_duration = flat_movie_duration(track, movie_timescale);
    if movie_duration > u64::from(u32::MAX) && !track_uses_direct_iamf_flat_timing(track) {
        tkhd.set_version(1);
        tkhd.duration_v1 = movie_duration;
    } else {
        tkhd.duration_v0 = movie_duration as u32;
    }
    tkhd.layer = 0;
    tkhd.alternate_group = track.config.alternate_group();
    tkhd.volume = track.config.volume();
    tkhd.matrix = track.config.matrix();
    tkhd.width = track
        .config
        .track_width_fixed_16_16()
        .unwrap_or_else(|| u32::from(track.config.track_width()) << 16);
    tkhd.height = track
        .config
        .track_height_fixed_16_16()
        .unwrap_or_else(|| u32::from(track.config.track_height()) << 16);
    Ok(tkhd)
}

fn build_mdia_bytes(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    ftyp_size: u64,
    mdat_header_size: u64,
    mdat_data_start: u64,
    auto_flat_creation_time: Option<u32>,
) -> Result<Vec<u8>, MuxError> {
    let mdhd = build_mdhd(track, auto_flat_creation_time)?;
    let hdlr = build_hdlr(track);
    let minf = build_minf_bytes(
        file_config,
        track,
        ftyp_size,
        mdat_header_size,
        mdat_data_start,
    )?;
    let children = [
        encode_typed_box(&mdhd, &[])?,
        encode_typed_box(&hdlr, &[])?,
        minf,
    ]
    .concat();
    encode_typed_box(&Mdia, &children)
}

fn build_mdhd_base(track: &PreparedTrack<'_>) -> Result<Mdhd, MuxError> {
    let mut mdhd = Mdhd::default();
    mdhd.timescale = track.config.timescale();
    let media_duration = track
        .flat_timing_override
        .map(|override_value| override_value.media_duration)
        .unwrap_or(track.media_duration);
    if media_duration > u64::from(u32::MAX) {
        mdhd.set_version(1);
        mdhd.duration_v1 = media_duration;
    } else {
        mdhd.duration_v0 =
            u32::try_from(media_duration).map_err(|_| MuxError::LayoutOverflow("mdhd duration"))?;
    }
    mdhd.language = encode_iso639_2_language(track.config)?;
    Ok(mdhd)
}

fn build_mdhd(
    track: &PreparedTrack<'_>,
    auto_flat_creation_time: Option<u32>,
) -> Result<Mdhd, MuxError> {
    let mut mdhd = build_mdhd_base(track)?;
    if let Some(auto_flat_creation_time) = auto_flat_creation_time {
        let modification_time = if track.config.kind() == MuxTrackKind::Video
            && track_uses_imported_authority_headers(track)
        {
            track
                .config
                .flat_source_media_modification_time()
                .unwrap_or(u64::from(auto_flat_creation_time))
        } else {
            u64::from(auto_flat_creation_time)
        };
        apply_flat_media_header_times(
            &mut mdhd,
            track.config.flat_source_media_creation_time(),
            modification_time,
        )?;
    }
    Ok(mdhd)
}

fn apply_flat_track_header_times(
    tkhd: &mut Tkhd,
    source_creation_time: Option<u64>,
    modification_time: u64,
) -> Result<(), MuxError> {
    let creation_time = source_creation_time.unwrap_or(modification_time);
    if tkhd.version() == 1
        || creation_time > u64::from(u32::MAX)
        || modification_time > u64::from(u32::MAX)
    {
        tkhd.set_version(1);
        tkhd.creation_time_v1 = creation_time;
        tkhd.modification_time_v1 = modification_time;
    } else {
        tkhd.creation_time_v0 = u32::try_from(creation_time)
            .map_err(|_| MuxError::LayoutOverflow("tkhd creation time"))?;
        tkhd.modification_time_v0 = u32::try_from(modification_time)
            .map_err(|_| MuxError::LayoutOverflow("tkhd modification time"))?;
    }
    Ok(())
}

fn apply_flat_movie_header_times(
    mvhd: &mut Mvhd,
    source_creation_time: Option<u64>,
    modification_time: u64,
) -> Result<(), MuxError> {
    let creation_time = source_creation_time.unwrap_or(modification_time);
    if mvhd.version() == 1
        || creation_time > u64::from(u32::MAX)
        || modification_time > u64::from(u32::MAX)
    {
        mvhd.set_version(1);
        mvhd.creation_time_v1 = creation_time;
        mvhd.modification_time_v1 = modification_time;
    } else {
        mvhd.creation_time_v0 = u32::try_from(creation_time)
            .map_err(|_| MuxError::LayoutOverflow("mvhd creation time"))?;
        mvhd.modification_time_v0 = u32::try_from(modification_time)
            .map_err(|_| MuxError::LayoutOverflow("mvhd modification time"))?;
    }
    Ok(())
}

fn apply_flat_media_header_times(
    mdhd: &mut Mdhd,
    source_creation_time: Option<u64>,
    modification_time: u64,
) -> Result<(), MuxError> {
    let creation_time = source_creation_time.unwrap_or(modification_time);
    if mdhd.version() == 1
        || creation_time > u64::from(u32::MAX)
        || modification_time > u64::from(u32::MAX)
    {
        mdhd.set_version(1);
        mdhd.creation_time_v1 = creation_time;
        mdhd.modification_time_v1 = modification_time;
    } else {
        mdhd.creation_time_v0 = u32::try_from(creation_time)
            .map_err(|_| MuxError::LayoutOverflow("mdhd creation time"))?;
        mdhd.modification_time_v0 = u32::try_from(modification_time)
            .map_err(|_| MuxError::LayoutOverflow("mdhd modification time"))?;
    }
    Ok(())
}

fn build_hdlr(track: &PreparedTrack<'_>) -> Hdlr {
    let mut hdlr = Hdlr::default();
    hdlr.handler_type = match track.config.kind() {
        MuxTrackKind::Audio => FourCc::from_bytes(*b"soun"),
        MuxTrackKind::Video => video_handler_type(track.config.sample_entry_box()),
        MuxTrackKind::Text => FourCc::from_bytes(*b"text"),
        MuxTrackKind::Subtitle => subtitle_handler_type(track.config.sample_entry_box()),
    };
    hdlr.name = track.config.handler_name().to_string();
    hdlr
}

fn video_handler_type(sample_entry_box: &[u8]) -> FourCc {
    if sample_entry_matches(sample_entry_box, &[b"mp4s"]) {
        SDSM
    } else {
        FourCc::from_bytes(*b"vide")
    }
}

fn subtitle_handler_type(sample_entry_box: &[u8]) -> FourCc {
    if sample_entry_box.len() >= 8
        && FourCc::from_bytes([
            sample_entry_box[4],
            sample_entry_box[5],
            sample_entry_box[6],
            sample_entry_box[7],
        ]) == FourCc::from_bytes(*b"mp4s")
    {
        FourCc::from_bytes(*b"subp")
    } else {
        FourCc::from_bytes(*b"subt")
    }
}

fn fragmented_mehd_duration(
    movie_timescale: u32,
    track: &PreparedTrack<'_>,
) -> Result<u64, MuxError> {
    let sample_entry_type = sample_entry_box_type(track.sample_entry_box)?;
    if sample_entry_type == FourCc::from_bytes(*b"vp08") {
        let mut duration = track.samples.iter().try_fold(0_u64, |duration, sample| {
            duration
                .checked_add(u64::from(sample.duration_movie))
                .ok_or(MuxError::LayoutOverflow("fragmented mehd duration"))
        });
        if track.config.timescale() == 30_000 {
            duration = duration.map(|value| value.saturating_sub(1));
        }
        return duration;
    }
    if track.config.kind() == MuxTrackKind::Audio {
        let summed_sample_duration = track.samples.iter().try_fold(0_u64, |duration, sample| {
            duration
                .checked_add(u64::from(sample.duration_movie))
                .ok_or(MuxError::LayoutOverflow("fragmented mehd duration"))
        })?;
        let should_trim_one_tick = if sample_entry_type == FourCc::from_bytes(*b"ec-3") {
            fragmented_ec3_mehd_trims_one_tick(
                track.sample_entry_box,
                track.config.timescale(),
                track.samples.len(),
            )?
        } else if sample_entry_type == FourCc::from_bytes(*b"mp4a") {
            fragmented_mp4a_mehd_trims_one_tick(track)?
        } else {
            false
        };
        return Ok(if should_trim_one_tick {
            summed_sample_duration.saturating_sub(1)
        } else {
            summed_sample_duration
        });
    }
    let media_duration = if track.config.kind() == MuxTrackKind::Audio {
        track.media_duration
    } else {
        track.presentation_duration_media
    };
    let mut duration = scale_track_time_to_movie(
        track.config.track_id(),
        i64::try_from(media_duration)
            .map_err(|_| MuxError::LayoutOverflow("fragmented mehd duration"))?,
        track.config.timescale(),
        movie_timescale,
    )
    .and_then(|value| {
        u64::try_from(value).map_err(|_| MuxError::LayoutOverflow("fragmented mehd duration"))
    })?;
    if fragmented_track_uses_trimmed_non_square_avc_pasp(track)? {
        duration = duration.saturating_sub(1);
    }
    Ok(duration)
}

fn build_minf_bytes(
    file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    ftyp_size: u64,
    mdat_header_size: u64,
    mdat_data_start: u64,
) -> Result<Vec<u8>, MuxError> {
    let media_header = match track.config.kind() {
        MuxTrackKind::Audio => {
            let smhd = Smhd::default();
            encode_typed_box(&smhd, &[])?
        }
        MuxTrackKind::Video => {
            let mut vmhd = Vmhd::default();
            vmhd.set_flags(VMHD_DEFAULT_FLAGS);
            encode_typed_box(&vmhd, &[])?
        }
        MuxTrackKind::Text => {
            let nmhd = Nmhd::default();
            encode_typed_box(&nmhd, &[])?
        }
        MuxTrackKind::Subtitle => {
            build_subtitle_media_header_bytes(track.config.sample_entry_box())?
        }
    };
    let dinf = build_dinf_bytes()?;
    let stbl = build_stbl_bytes(
        file_config,
        track,
        ftyp_size,
        mdat_header_size,
        mdat_data_start,
    )?;
    encode_typed_box(&Minf, &[media_header, dinf, stbl].concat())
}

fn build_dinf_bytes() -> Result<Vec<u8>, MuxError> {
    let mut url = Url::default();
    url.set_flags(0x0000_0001);
    let mut dref = Dref::default();
    dref.entry_count = 1;
    let dref_children = encode_typed_box(&url, &[])?;
    let dref_bytes = encode_typed_box(&dref, &dref_children)?;
    encode_typed_box(&Dinf, &dref_bytes)
}

fn build_subtitle_media_header_bytes(sample_entry_box: &[u8]) -> Result<Vec<u8>, MuxError> {
    if sample_entry_matches(sample_entry_box, &[b"mp4s"]) {
        return encode_typed_box(&Nmhd::default(), &[]);
    }
    encode_typed_box(&Sthd::default(), &[])
}

fn build_stbl_bytes(
    _file_config: &MuxFileConfig,
    track: &PreparedTrack<'_>,
    _ftyp_size: u64,
    _mdat_header_size: u64,
    mdat_data_start: u64,
) -> Result<Vec<u8>, MuxError> {
    let stsd = build_stsd_bytes(track)?;
    let stts = build_stts(track)?;
    let stsc = preserved_flat_stsc_or_built(track)?;
    let stsz = build_stsz(track)?;
    let chunk_offsets = build_chunk_offsets(track, mdat_data_start)?;
    let mut has_preserved_sbgp = false;
    let mut has_preserved_sgpd = false;
    let mut preserved_cslg_boxes = Vec::new();
    let mut preserved_other_boxes = Vec::new();
    for box_bytes in track.config.preserved_flat_stbl_boxes().iter().cloned() {
        match box_bytes
            .get(4..8)
            .and_then(|box_type| box_type.try_into().ok())
            .map(FourCc::from_bytes)
        {
            Some(CSLG) => preserved_cslg_boxes.push(box_bytes),
            Some(SBGP) => {
                has_preserved_sbgp = true;
                preserved_other_boxes.push(box_bytes);
            }
            Some(SGPD) => {
                has_preserved_sgpd = true;
                preserved_other_boxes.push(box_bytes);
            }
            _ => preserved_other_boxes.push(box_bytes),
        }
    }
    let mut children = vec![stsd, encode_typed_box(&stts, &[])?];
    if let Some(ctts) = build_ctts(track)? {
        children.push(encode_typed_box(&ctts, &[])?);
    }
    children.extend(preserved_cslg_boxes);
    if let Some(stss) = build_stss(track)? {
        children.push(encode_typed_box(&stss, &[])?);
    }
    children.push(encode_typed_box(&stsc, &[])?);
    children.push(encode_typed_box(&stsz, &[])?);
    if chunk_offsets
        .iter()
        .all(|offset| *offset <= u64::from(u32::MAX))
    {
        children.push(encode_typed_box(&build_stco(&chunk_offsets)?, &[])?);
    } else {
        children.push(encode_typed_box(&build_co64(&chunk_offsets)?, &[])?);
    }
    if let Some(sample_roll_distance) = track.config.sample_roll_distance() {
        if !has_preserved_sgpd {
            children.push(encode_typed_box(
                &build_roll_sgpd(sample_roll_distance),
                &[],
            )?);
        }
        if track.config.emit_roll_sbgp() && !has_preserved_sbgp {
            children.push(encode_typed_box(
                &build_roll_sbgp(
                    u32::try_from(track.samples.len())
                        .map_err(|_| MuxError::LayoutOverflow("roll sample count"))?,
                ),
                &[],
            )?);
        }
    }
    children.extend(preserved_other_boxes);

    encode_typed_box(&Stbl, &children.concat())
}

fn preserved_flat_stsc_or_built(track: &PreparedTrack<'_>) -> Result<Stsc, MuxError> {
    if let Some(stsc) = track.config.flat_stsc_override()
        && stsc_matches_chunk_sample_counts(
            stsc,
            &track.chunk_sample_counts,
            track.config.sample_entry_boxes().len(),
        )
    {
        return Ok(stsc.clone());
    }
    build_stsc(track)
}

fn stsc_matches_chunk_sample_counts(
    stsc: &Stsc,
    chunk_sample_counts: &[u32],
    sample_entry_count: usize,
) -> bool {
    let mut expanded = Vec::with_capacity(chunk_sample_counts.len());
    for (index, entry) in stsc.entries.iter().enumerate() {
        if entry.first_chunk == 0
            || entry.sample_description_index == 0
            || usize::try_from(entry.sample_description_index)
                .ok()
                .is_none_or(|index| index > sample_entry_count)
        {
            return false;
        }
        let next_first_chunk = stsc
            .entries
            .get(index + 1)
            .map(|next| next.first_chunk)
            .unwrap_or_else(|| {
                u32::try_from(chunk_sample_counts.len())
                    .unwrap_or(u32::MAX)
                    .saturating_add(1)
            });
        if next_first_chunk <= entry.first_chunk {
            return false;
        }
        for _ in entry.first_chunk..next_first_chunk {
            expanded.push(entry.samples_per_chunk);
        }
    }
    expanded == chunk_sample_counts
}

fn build_stsd_bytes(track: &PreparedTrack<'_>) -> Result<Vec<u8>, MuxError> {
    let mut stsd = Stsd::default();
    stsd.entry_count = u32::try_from(track.config.sample_entry_boxes().len())
        .map_err(|_| MuxError::LayoutOverflow("stsd entry_count"))?;
    encode_typed_box(&stsd, &track.config.sample_entry_boxes().concat())
}

fn build_stts(track: &PreparedTrack<'_>) -> Result<Stts, MuxError> {
    let entries = if let Some(override_value) = track.flat_timing_override {
        encode_stts_runs(
            track.config.stts_run_encoding_mode(),
            override_value.sample_durations.iter().copied(),
        )
    } else {
        encode_stts_runs(
            track.config.stts_run_encoding_mode(),
            track.samples.iter().map(|sample| sample.duration_media),
        )
    };
    let mut stts = Stts::default();
    stts.entry_count =
        u32::try_from(entries.len()).map_err(|_| MuxError::LayoutOverflow("stts entry_count"))?;
    stts.entries = entries
        .into_iter()
        .map(|(sample_count, sample_delta)| SttsEntry {
            sample_count,
            sample_delta,
        })
        .collect();
    Ok(stts)
}

fn encode_stts_runs<I>(encoding_mode: super::SttsRunEncodingMode, values: I) -> Vec<(u32, u32)>
where
    I: IntoIterator<Item = u32>,
{
    match encoding_mode {
        super::SttsRunEncodingMode::CollapseIdentical => run_length_encode_u32(values),
        super::SttsRunEncodingMode::PreservePerSample => {
            values.into_iter().map(|value| (1, value)).collect()
        }
    }
}

fn build_ctts(track: &PreparedTrack<'_>) -> Result<Option<Ctts>, MuxError> {
    if track
        .samples
        .iter()
        .all(|sample| sample.composition_offset_media == 0)
    {
        return Ok(None);
    }

    let use_version_one = track
        .samples
        .iter()
        .any(|sample| sample.composition_offset_media < 0);
    let runs = run_length_encode_i32(
        track
            .samples
            .iter()
            .map(|sample| sample.composition_offset_media),
    );
    let mut ctts = Ctts::default();
    if use_version_one {
        ctts.set_version(1);
    }
    ctts.entry_count =
        u32::try_from(runs.len()).map_err(|_| MuxError::LayoutOverflow("ctts entry_count"))?;
    ctts.entries = runs
        .into_iter()
        .map(|(sample_count, sample_offset)| CttsEntry {
            sample_count,
            sample_offset_v0: u32::try_from(sample_offset).unwrap_or(0),
            sample_offset_v1: sample_offset,
        })
        .collect();
    Ok(Some(ctts))
}

fn build_stsc(track: &PreparedTrack<'_>) -> Result<Stsc, MuxError> {
    let encoded_runs = build_stsc_runs(track)?;
    let mut stsc = Stsc::default();
    stsc.entry_count = u32::try_from(encoded_runs.len())
        .map_err(|_| MuxError::LayoutOverflow("stsc entry_count"))?;
    let mut first_chunk = 1_u32;
    stsc.entries = Vec::with_capacity(encoded_runs.len());
    for (chunk_run_length, samples_per_chunk, sample_description_index) in encoded_runs {
        stsc.entries.push(StscEntry {
            first_chunk,
            samples_per_chunk,
            sample_description_index,
        });
        first_chunk = first_chunk
            .checked_add(chunk_run_length)
            .ok_or(MuxError::LayoutOverflow("stsc first_chunk"))?;
    }
    Ok(stsc)
}

fn build_stsc_runs(track: &PreparedTrack<'_>) -> Result<Vec<(u32, u32, u32)>, MuxError> {
    let chunk_mappings = flat_chunk_sample_description_mappings(track)?;
    let mut encoded_runs = run_length_encode_stsc_mappings(chunk_mappings);
    if track.config.stsc_run_encoding_mode() == super::StscRunEncodingMode::PreserveTerminalBoundary
        && track.chunk_sample_counts.len() > 1
        && let Some((run_length, samples_per_chunk, sample_description_index)) =
            encoded_runs.last().copied()
        && run_length > 1
    {
        encoded_runs.pop();
        encoded_runs.push((run_length - 1, samples_per_chunk, sample_description_index));
        encoded_runs.push((1, samples_per_chunk, sample_description_index));
    }
    Ok(encoded_runs)
}

fn flat_chunk_sample_description_mappings(
    track: &PreparedTrack<'_>,
) -> Result<Vec<(u32, u32)>, MuxError> {
    let mut mappings = Vec::with_capacity(track.chunk_sample_counts.len());
    let mut sample_index = 0_usize;
    for &samples_per_chunk in &track.chunk_sample_counts {
        let chunk_len = usize::try_from(samples_per_chunk)
            .map_err(|_| MuxError::LayoutOverflow("stsc sample count"))?;
        let chunk_end = sample_index
            .checked_add(chunk_len)
            .ok_or(MuxError::LayoutOverflow("stsc sample indexing"))?;
        let chunk_samples = track.samples.get(sample_index..chunk_end).ok_or_else(|| {
            MuxError::InvalidChunkPlan {
                track_id: track.config.track_id(),
                message: "chunk boundaries ran past the staged sample count".to_string(),
            }
        })?;
        let sample_description_index = chunk_samples
            .first()
            .map(|sample| sample.sample_description_index)
            .unwrap_or(1);
        if sample_description_index == 0
            || usize::try_from(sample_description_index)
                .ok()
                .is_none_or(|index| index > track.config.sample_entry_boxes().len())
        {
            return Err(MuxError::InvalidOutputLayout {
                layout: "flat",
                message: format!(
                    "track {} uses sample description index {} with {} sample entries",
                    track.config.track_id(),
                    sample_description_index,
                    track.config.sample_entry_boxes().len()
                ),
            });
        }
        if !chunk_samples
            .iter()
            .all(|sample| sample.sample_description_index == sample_description_index)
        {
            return Err(MuxError::InvalidOutputLayout {
                layout: "flat",
                message: format!(
                    "track {} has mixed sample descriptions inside one chunk",
                    track.config.track_id()
                ),
            });
        }
        mappings.push((samples_per_chunk, sample_description_index));
        sample_index = chunk_end;
    }
    Ok(mappings)
}

fn run_length_encode_stsc_mappings<I>(values: I) -> Vec<(u32, u32, u32)>
where
    I: IntoIterator<Item = (u32, u32)>,
{
    let mut runs = Vec::new();
    for (samples_per_chunk, sample_description_index) in values {
        if let Some((run_length, previous_samples_per_chunk, previous_description_index)) =
            runs.last_mut()
            && *previous_samples_per_chunk == samples_per_chunk
            && *previous_description_index == sample_description_index
        {
            *run_length += 1;
            continue;
        }
        runs.push((1, samples_per_chunk, sample_description_index));
    }
    runs
}

fn build_stsz(track: &PreparedTrack<'_>) -> Result<Stsz, MuxError> {
    let mut stsz = Stsz::default();
    stsz.sample_count =
        u32::try_from(track.samples.len()).map_err(|_| MuxError::LayoutOverflow("sample_count"))?;
    if let Some(sample_size) = all_equal_u64(track.samples.iter().map(|sample| sample.sample_size))
    {
        stsz.sample_size =
            u32::try_from(sample_size).map_err(|_| MuxError::LayoutOverflow("sample size"))?;
    } else {
        stsz.sample_size = 0;
        stsz.entry_size = track
            .samples
            .iter()
            .map(|sample| sample.sample_size)
            .collect();
    }
    Ok(stsz)
}

fn build_chunk_offsets(
    track: &PreparedTrack<'_>,
    mdat_data_start: u64,
) -> Result<Vec<u64>, MuxError> {
    let mut chunk_offsets = Vec::with_capacity(track.chunk_sample_counts.len());
    let mut sample_index = 0_usize;
    for &samples_per_chunk in &track.chunk_sample_counts {
        let sample = track
            .samples
            .get(sample_index)
            .ok_or_else(|| MuxError::InvalidChunkPlan {
                track_id: track.config.track_id(),
                message: "chunk boundaries ran past the staged sample count".to_string(),
            })?;
        chunk_offsets.push(
            mdat_data_start
                .checked_add(sample.output_offset)
                .ok_or(MuxError::LayoutOverflow("chunk offset"))?,
        );
        sample_index = sample_index
            .checked_add(
                usize::try_from(samples_per_chunk)
                    .map_err(|_| MuxError::LayoutOverflow("chunk sample-count conversion"))?,
            )
            .ok_or(MuxError::LayoutOverflow("chunk sample indexing"))?;
    }
    Ok(chunk_offsets)
}

fn build_stco(chunk_offsets: &[u64]) -> Result<Stco, MuxError> {
    let mut stco = Stco::default();
    stco.entry_count =
        u32::try_from(chunk_offsets.len()).map_err(|_| MuxError::LayoutOverflow("chunk_count"))?;
    for &offset in chunk_offsets {
        let _ = u32::try_from(offset).map_err(|_| MuxError::LayoutOverflow("chunk offset"))?;
    }
    stco.chunk_offset = chunk_offsets.to_vec();
    Ok(stco)
}

fn build_co64(chunk_offsets: &[u64]) -> Result<Co64, MuxError> {
    let mut co64 = Co64::default();
    co64.entry_count =
        u32::try_from(chunk_offsets.len()).map_err(|_| MuxError::LayoutOverflow("chunk_count"))?;
    co64.chunk_offset = chunk_offsets.to_vec();
    Ok(co64)
}

fn build_stss(track: &PreparedTrack<'_>) -> Result<Option<Stss>, MuxError> {
    if matches!(
        track.config.sync_sample_table_mode,
        super::SyncSampleTableMode::ForceEmpty
    ) {
        return Ok(Some(Stss::default()));
    }

    if track.samples.iter().all(|sample| sample.is_sync_sample)
        && !matches!(
            track.config.sync_sample_table_mode,
            super::SyncSampleTableMode::ForceFirstOnly
        )
    {
        return Ok(None);
    }

    let mut stss = Stss::default();
    stss.sample_number = match track.config.sync_sample_table_mode {
        super::SyncSampleTableMode::ForceFirstOnly => track
            .samples
            .iter()
            .enumerate()
            .find_map(|(index, sample)| {
                sample
                    .is_sync_sample
                    .then_some(u64::try_from(index + 1).ok())
                    .flatten()
            })
            .into_iter()
            .collect(),
        _ => track
            .samples
            .iter()
            .enumerate()
            .filter_map(|(index, sample)| {
                sample
                    .is_sync_sample
                    .then_some(u64::try_from(index + 1).ok())
                    .flatten()
            })
            .collect(),
    };
    stss.entry_count = u32::try_from(stss.sample_number.len())
        .map_err(|_| MuxError::LayoutOverflow("stss entry_count"))?;
    Ok(Some(stss))
}

fn track_uses_imported_authority_headers(track: &PreparedTrack<'_>) -> bool {
    track.config.flat_source_track_creation_time().is_some()
        || track.config.flat_source_media_creation_time().is_some()
}

fn track_uses_direct_iamf_flat_timing(track: &PreparedTrack<'_>) -> bool {
    sample_entry_matches(track.sample_entry_box, &[b"iamf"])
        && !track_uses_imported_authority_headers(track)
        && track.flat_timing_override.is_some()
}

pub(super) fn build_visual_random_access_sgpd() -> Sgpd {
    let mut sgpd = Sgpd::default();
    sgpd.set_version(1);
    sgpd.grouping_type = FourCc::from_bytes(*b"rap ");
    sgpd.default_length = 1;
    sgpd.entry_count = 1;
    sgpd.visual_random_access_entries = vec![VisualRandomAccessEntry {
        num_leading_samples_known: false,
        num_leading_samples: 0,
    }];
    sgpd
}

pub(super) fn build_visual_random_access_sbgp(entries: Vec<SbgpEntry>) -> Result<Sbgp, MuxError> {
    let mut sbgp = Sbgp::default();
    sbgp.grouping_type = u32::from_be_bytes(*b"rap ");
    sbgp.entry_count = u32::try_from(entries.len())
        .map_err(|_| MuxError::LayoutOverflow("rap sbgp entry_count"))?;
    sbgp.entries = entries;
    Ok(sbgp)
}

fn build_roll_sgpd(sample_roll_distance: i16) -> Sgpd {
    let mut sgpd = Sgpd::default();
    sgpd.set_version(1);
    sgpd.grouping_type = FourCc::from_bytes(*b"roll");
    sgpd.default_length = 2;
    sgpd.entry_count = 1;
    sgpd.roll_distances = vec![sample_roll_distance];
    sgpd
}

fn build_roll_sbgp(sample_count: u32) -> Sbgp {
    let mut sbgp = Sbgp::default();
    sbgp.grouping_type = u32::from_be_bytes(*b"roll");
    sbgp.entry_count = 1;
    sbgp.entries = vec![SbgpEntry {
        sample_count,
        group_description_index: 1,
    }];
    sbgp
}

pub(super) fn encode_typed_box<B>(box_value: &B, children: &[u8]) -> Result<Vec<u8>, MuxError>
where
    B: CodecBox,
{
    encode_typed_box_as(box_value.box_type(), box_value, children)
}

fn encode_typed_box_as<B>(
    box_type: FourCc,
    box_value: &B,
    children: &[u8],
) -> Result<Vec<u8>, MuxError>
where
    B: CodecBox,
{
    let mut payload = Vec::new();
    marshal(&mut payload, box_value, None)?;
    payload.extend_from_slice(children);
    encode_raw_box(box_type, &payload)
}

pub(super) fn encode_raw_box(box_type: FourCc, payload: &[u8]) -> Result<Vec<u8>, MuxError> {
    let mut cursor = Cursor::new(Vec::new());
    let payload_size =
        u64::try_from(payload.len()).map_err(|_| MuxError::LayoutOverflow("box payload"))?;
    let header = BoxInfo::new(box_type, BoxInfo::new(box_type, 8).size() + payload_size);
    let written = header.write(&mut cursor)?;
    if written.payload_size()? != payload_size {
        return Err(MuxError::LayoutOverflow("box header normalization"));
    }
    cursor.get_mut().extend_from_slice(payload);
    Ok(cursor.into_inner())
}

fn encode_header_only(
    box_type: FourCc,
    payload_size: u64,
    field_name: &'static str,
) -> Result<Vec<u8>, MuxError> {
    let mut cursor = Cursor::new(Vec::new());
    let header = BoxInfo::new(
        box_type,
        BoxInfo::new(box_type, 8)
            .size()
            .checked_add(payload_size)
            .ok_or(MuxError::LayoutOverflow(field_name))?,
    );
    header.write(&mut cursor)?;
    Ok(cursor.into_inner())
}

fn validate_sample_entry_box(config: &MuxTrackConfig) -> Result<(), MuxError> {
    if config.sample_entry_boxes().is_empty() {
        return Err(MuxError::InvalidSampleEntryBox {
            track_id: config.track_id(),
            message: "expected at least one encoded sample-entry box".to_string(),
        });
    }
    for sample_entry_box in config.sample_entry_boxes() {
        let mut cursor = Cursor::new(sample_entry_box);
        let info = BoxInfo::read(&mut cursor).map_err(|error| MuxError::InvalidSampleEntryBox {
            track_id: config.track_id(),
            message: error.to_string(),
        })?;
        let end = usize::try_from(info.size()).map_err(|_| MuxError::InvalidSampleEntryBox {
            track_id: config.track_id(),
            message: "box size is too large".to_string(),
        })?;
        if info.extend_to_eof() || end != sample_entry_box.len() {
            return Err(MuxError::InvalidSampleEntryBox {
                track_id: config.track_id(),
                message: "expected complete encoded sample-entry boxes".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_language(config: &MuxTrackConfig) -> Result<(), MuxError> {
    let language = config.language();
    if language.iter().all(|byte| byte.is_ascii_lowercase()) {
        return Ok(());
    }
    Err(MuxError::InvalidTrackLanguage {
        track_id: config.track_id(),
        language: String::from_utf8_lossy(&language).into_owned(),
    })
}

fn encode_iso639_2_language(config: &MuxTrackConfig) -> Result<[u8; 3], MuxError> {
    let language = config.language();
    if !language.iter().all(|byte| byte.is_ascii_lowercase()) {
        return Err(MuxError::InvalidTrackLanguage {
            track_id: config.track_id(),
            language: String::from_utf8_lossy(&language).into_owned(),
        });
    }
    Ok([language[0] - b'`', language[1] - b'`', language[2] - b'`'])
}

fn scale_movie_time_to_track(
    track_id: u32,
    value: u64,
    movie_timescale: u32,
    track_timescale: u32,
) -> Result<u64, MuxError> {
    if track_timescale == 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }
    if movie_timescale == track_timescale {
        return Ok(value);
    }
    let scaled = value
        .checked_mul(u64::from(track_timescale))
        .ok_or(MuxError::LayoutOverflow("track time scaling"))?;
    if scaled % u64::from(movie_timescale) != 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }
    Ok(scaled / u64::from(movie_timescale))
}

fn scale_track_time_to_movie(
    track_id: u32,
    value: i64,
    track_timescale: u32,
    movie_timescale: u32,
) -> Result<i64, MuxError> {
    if track_timescale == 0 || movie_timescale == 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }
    let sign = value.signum();
    let magnitude = value.unsigned_abs();
    let scaled = magnitude
        .checked_mul(u64::from(movie_timescale))
        .ok_or(MuxError::LayoutOverflow("movie time scaling"))?;
    if scaled % u64::from(track_timescale) != 0 {
        return Err(MuxError::InvalidTrackTimescale { track_id });
    }
    i64::try_from(scaled / u64::from(track_timescale))
        .map(|normalized| normalized * sign)
        .map_err(|_| MuxError::LayoutOverflow("movie time scaling"))
}

fn scale_movie_offset_to_track(
    track_id: u32,
    value: i64,
    movie_timescale: u32,
    track_timescale: u32,
) -> Result<i32, MuxError> {
    if value == 0 {
        return Ok(0);
    }

    let sign = value.signum();
    let magnitude =
        u64::try_from(value.abs()).map_err(|_| MuxError::LayoutOverflow("composition offset"))?;
    let scaled =
        scale_movie_time_to_track(track_id, magnitude, movie_timescale, track_timescale)? as i64;
    let signed = scaled
        .checked_mul(sign)
        .ok_or(MuxError::LayoutOverflow("composition offset"))?;
    i32::try_from(signed).map_err(|_| MuxError::LayoutOverflow("composition offset"))
}

fn run_length_encode_u32<I>(values: I) -> Vec<(u32, u32)>
where
    I: IntoIterator<Item = u32>,
{
    let mut runs = Vec::new();
    for value in values {
        match runs.last_mut() {
            Some((sample_count, last_value)) if *last_value == value => {
                *sample_count += 1;
            }
            _ => runs.push((1, value)),
        }
    }
    runs
}

fn run_length_encode_i32<I>(values: I) -> Vec<(u32, i32)>
where
    I: IntoIterator<Item = i32>,
{
    let mut runs = Vec::new();
    for value in values {
        match runs.last_mut() {
            Some((sample_count, last_value)) if *last_value == value => {
                *sample_count += 1;
            }
            _ => runs.push((1, value)),
        }
    }
    runs
}

fn canonicalize_fragmented_sample_entry_box(sample_entry_box: &[u8]) -> Result<Vec<u8>, MuxError> {
    let sample_entry_type = sample_entry_box_type(sample_entry_box)?;
    match sample_entry_type {
        value
            if value == FourCc::from_bytes(*b"avc1")
                || value == FourCc::from_bytes(*b"avc2")
                || value == FourCc::from_bytes(*b"avc3")
                || value == FourCc::from_bytes(*b"avc4") =>
        {
            canonicalize_fragmented_visual_sample_entry_box(sample_entry_box, "AVC Coding", &[])
        }
        value if value == FourCc::from_bytes(*b"hvc1") || value == FourCc::from_bytes(*b"hev1") => {
            canonicalize_fragmented_hevc_sample_entry_box(
                sample_entry_box,
                "HEVC Coding",
                &[FourCc::from_bytes(*b"fiel")],
            )
        }
        value if value == FourCc::from_bytes(*b"dvh1") || value == FourCc::from_bytes(*b"dvhe") => {
            canonicalize_fragmented_hevc_sample_entry_box(
                sample_entry_box,
                "DOVI Coding",
                &[FourCc::from_bytes(*b"fiel")],
            )
        }
        value if value == FourCc::from_bytes(*b"vvc1") || value == FourCc::from_bytes(*b"vvi1") => {
            canonicalize_fragmented_visual_sample_entry_box(
                sample_entry_box,
                "VVC Coding",
                &[FourCc::from_bytes(*b"fiel")],
            )
        }
        value if value == FourCc::from_bytes(*b"av01") => {
            if sample_entry_carries_child_type(
                sample_entry_box,
                &[FourCc::from_bytes(*b"dvcC"), FourCc::from_bytes(*b"dvvC")],
            ) {
                canonicalize_fragmented_av1_extended_sample_entry_box(sample_entry_box)
            } else {
                canonicalize_fragmented_visual_sample_entry_box(
                    sample_entry_box,
                    "AOM Coding",
                    &[FourCc::from_bytes(*b"fiel"), FourCc::from_bytes(*b"pasp")],
                )
            }
        }
        value
            if value == FourCc::from_bytes(*b"vp08")
                || value == FourCc::from_bytes(*b"vp09")
                || value == FourCc::from_bytes(*b"vp10") =>
        {
            canonicalize_fragmented_visual_sample_entry_box(
                sample_entry_box,
                "VPC Coding",
                &[
                    FourCc::from_bytes(*b"fiel"),
                    FourCc::from_bytes(*b"pasp"),
                    FourCc::from_bytes(*b"btrt"),
                ],
            )
        }
        value if value == FourCc::from_bytes(*b"mp4a") => {
            canonicalize_fragmented_audio_sample_entry_box(sample_entry_box, true, &[])
        }
        value if value == FourCc::from_bytes(*b"alac") => {
            canonicalize_fragmented_audio_sample_entry_box(
                sample_entry_box,
                false,
                &[FourCc::from_bytes(*b"btrt")],
            )
        }
        value if value == FourCc::from_bytes(*b"fLaC") => {
            let mut stripped_children = vec![FourCc::from_bytes(*b"btrt")];
            if sample_entry_audio_sample_rate_int(sample_entry_box) == Some(1_000) {
                stripped_children.push(FourCc::from_bytes(*b"dfLa"));
            }
            canonicalize_fragmented_audio_sample_entry_box(
                sample_entry_box,
                false,
                &stripped_children,
            )
        }
        value
            if value == FourCc::from_bytes(*b"ac-3")
                || value == FourCc::from_bytes(*b"ec-3")
                || value == FourCc::from_bytes(*b"ac-4")
                || value == FourCc::from_bytes(*b"Opus") =>
        {
            canonicalize_fragmented_audio_sample_entry_box(
                sample_entry_box,
                false,
                &[FourCc::from_bytes(*b"btrt")],
            )
        }
        value
            if value == FourCc::from_bytes(*b"dtsc")
                || value == FourCc::from_bytes(*b"dtse")
                || value == FourCc::from_bytes(*b"dtsh")
                || value == FourCc::from_bytes(*b"dtsl")
                || value == FourCc::from_bytes(*b"dtsm")
                || value == FourCc::from_bytes(*b"dts-")
                || value == FourCc::from_bytes(*b"dtsx")
                || value == FourCc::from_bytes(*b"dtsy") =>
        {
            canonicalize_fragmented_audio_sample_entry_box(
                sample_entry_box,
                false,
                &[FourCc::from_bytes(*b"btrt")],
            )
        }
        value
            if value == FourCc::from_bytes(*b"mha1")
                || value == FourCc::from_bytes(*b"mha2")
                || value == FourCc::from_bytes(*b"mhm1")
                || value == FourCc::from_bytes(*b"mhm2") =>
        {
            canonicalize_fragmented_audio_sample_entry_box(
                sample_entry_box,
                false,
                &[FourCc::from_bytes(*b"btrt")],
            )
        }
        _ => Ok(sample_entry_box.to_vec()),
    }
}

fn canonicalize_fragmented_av1_extended_sample_entry_box(
    sample_entry_box: &[u8],
) -> Result<Vec<u8>, MuxError> {
    let (mut sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    sample_entry.compressorname = encode_compressor_name("AOM Coding");

    let stripped_children = [
        FourCc::from_bytes(*b"fiel"),
        FourCc::from_bytes(*b"pasp"),
        FourCc::from_bytes(*b"btrt"),
        FourCc::from_bytes(*b"clli"),
        FourCc::from_bytes(*b"mdcv"),
    ];
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        if stripped_children.contains(&sample_entry_box_type(&child_box)?) {
            continue;
        }
        normalized_children.push(child_box);
    }
    let normalized_children = reorder_sample_entry_children_by_type_preference(
        &normalized_children,
        &[
            FourCc::from_bytes(*b"av1C"),
            FourCc::from_bytes(*b"dvcC"),
            FourCc::from_bytes(*b"dvvC"),
            FourCc::from_bytes(*b"colr"),
        ],
    );

    let mut child_payload = normalized_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

fn reorder_sample_entry_children_by_type_preference(
    child_boxes: &[Vec<u8>],
    preferred_order: &[FourCc],
) -> Vec<Vec<u8>> {
    let mut ordered = Vec::with_capacity(child_boxes.len());
    let mut used = vec![false; child_boxes.len()];
    for preferred_type in preferred_order {
        for (index, child_box) in child_boxes.iter().enumerate() {
            if used[index] || sample_entry_box_type(child_box).ok() != Some(*preferred_type) {
                continue;
            }
            ordered.push(child_box.clone());
            used[index] = true;
        }
    }
    for (index, child_box) in child_boxes.iter().enumerate() {
        if !used[index] {
            ordered.push(child_box.clone());
        }
    }
    ordered
}

fn canonicalize_fragmented_visual_sample_entry_box(
    sample_entry_box: &[u8],
    compressor_name: &str,
    stripped_children: &[FourCc],
) -> Result<Vec<u8>, MuxError> {
    let (mut sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    sample_entry.compressorname = encode_compressor_name(compressor_name);

    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        if stripped_children.contains(&sample_entry_box_type(&child_box)?) {
            continue;
        }
        normalized_children.push(child_box);
    }

    let mut child_payload = normalized_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

fn canonicalize_fragmented_hevc_sample_entry_box(
    sample_entry_box: &[u8],
    compressor_name: &str,
    stripped_children: &[FourCc],
) -> Result<Vec<u8>, MuxError> {
    let (mut sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    sample_entry.compressorname = encode_compressor_name(compressor_name);

    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        let child_type = sample_entry_box_type(&child_box)?;
        if stripped_children.contains(&child_type) {
            continue;
        }
        if child_type == FourCc::from_bytes(*b"pasp") && is_square_pasp_box(&child_box)? {
            continue;
        }
        normalized_children.push(child_box);
    }

    let mut child_payload = normalized_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

fn canonicalize_fragmented_audio_sample_entry_box(
    sample_entry_box: &[u8],
    normalize_esds: bool,
    stripped_children: &[FourCc],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, child_boxes, trailing_bytes) =
        decode_audio_sample_entry_parts(sample_entry_box)?;
    let sample_entry_type = sample_entry.sample_entry.box_type;
    let normalized_sample_rate = if sample_entry_type == FourCc::from_bytes(*b"mp4a") {
        fragmented_mp4a_sample_entry_sample_rate(sample_entry_box)?
    } else {
        sample_entry.sample_rate
    };
    let normalized_channel_count = if sample_entry_type == FourCc::from_bytes(*b"ec-3") {
        2
    } else {
        sample_entry.channel_count
    };
    let normalized_sample_entry = AudioSampleEntry {
        sample_entry: SampleEntry {
            box_type: sample_entry_type,
            data_reference_index: 1,
        },
        entry_version: sample_entry.entry_version,
        channel_count: normalized_channel_count,
        sample_size: sample_entry.sample_size,
        pre_defined: sample_entry.pre_defined,
        sample_rate: normalized_sample_rate,
        quicktime_data: sample_entry.quicktime_data.clone(),
    };
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        let child_type = sample_entry_box_type(&child_box)?;
        if stripped_children.contains(&child_type) {
            continue;
        }
        if normalize_esds && child_type == FourCc::from_bytes(*b"esds") {
            normalized_children.push(canonicalize_fragmented_esds_box(&child_box)?);
        } else {
            normalized_children.push(child_box);
        }
    }

    let mut child_payload = normalized_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&normalized_sample_entry, &child_payload)
}

fn fragmented_mp4a_sample_entry_sample_rate(sample_entry_box: &[u8]) -> Result<u32, MuxError> {
    let (sample_entry, child_boxes, _) = decode_audio_sample_entry_parts(sample_entry_box)?;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"esds") {
            continue;
        }
        let esds = decode_typed_box::<Esds>(&child_box)?;
        if let Ok(Some(sample_rate)) = detect_aac_effective_sample_rate(&esds) {
            return Ok(sample_rate << 16);
        }
    }
    Ok(sample_entry.sample_rate)
}

pub(crate) fn append_audio_sample_entry_btrt(
    sample_entry_box: &[u8],
    btrt: &Btrt,
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, child_boxes, trailing_bytes) =
        decode_audio_sample_entry_parts(sample_entry_box)?;
    if child_boxes.iter().any(|child_box| {
        sample_entry_box_type(child_box).ok() == Some(FourCc::from_bytes(*b"btrt"))
    }) {
        return Ok(sample_entry_box.to_vec());
    }

    let mut child_payload = child_boxes.concat();
    child_payload.extend_from_slice(&encode_typed_box(btrt, &[])?);
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn replace_audio_sample_entry_btrt(
    sample_entry_box: &[u8],
    btrt: &Btrt,
) -> Result<Vec<u8>, MuxError> {
    let stripped = strip_audio_sample_entry_immediate_children(
        sample_entry_box,
        &[FourCc::from_bytes(*b"btrt")],
    )?;
    append_audio_sample_entry_btrt(&stripped, btrt)
}

pub(crate) fn append_audio_sample_entry_child_box(
    sample_entry_box: &[u8],
    child_box: &[u8],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, child_boxes, trailing_bytes) =
        decode_audio_sample_entry_parts(sample_entry_box)?;
    let mut child_payload = child_boxes.concat();
    child_payload.extend_from_slice(child_box);
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn audio_sample_entry_vendor_code(
    sample_entry_box: &[u8],
) -> Result<Option<[u8; 4]>, MuxError> {
    let sample_entry = decode_audio_sample_entry(sample_entry_box)?;
    if sample_entry_box.len() < 24 {
        return Err(MuxError::UnsupportedTrackImport {
            spec: "<sample-entry>".to_string(),
            message: "audio sample entry is truncated before the vendor field".to_string(),
        });
    }
    if sample_entry.entry_version != 0 {
        return Ok(None);
    }
    let sample_entry_type = sample_entry.sample_entry.box_type;
    if sample_entry_type != FourCc::from_bytes(*b"ipcm")
        && sample_entry_type != FourCc::from_bytes(*b"fpcm")
        && sample_entry_type != FourCc::from_bytes(*b"spex")
    {
        return Ok(None);
    }
    Ok(Some(sample_entry_box[20..24].try_into().unwrap()))
}

pub(crate) fn replace_audio_sample_entry_vendor_code(
    sample_entry_box: &[u8],
    vendor_code: [u8; 4],
) -> Result<Vec<u8>, MuxError> {
    let Some(_) = audio_sample_entry_vendor_code(sample_entry_box)? else {
        return Ok(sample_entry_box.to_vec());
    };
    let mut replaced = sample_entry_box.to_vec();
    replaced[20..24].copy_from_slice(&vendor_code);
    Ok(replaced)
}

pub(crate) fn append_visual_sample_entry_btrt(
    sample_entry_box: &[u8],
    btrt: &Btrt,
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    if child_boxes.iter().any(|child_box| {
        sample_entry_box_type(child_box).ok() == Some(FourCc::from_bytes(*b"btrt"))
    }) {
        return Ok(sample_entry_box.to_vec());
    }

    let mut child_payload = child_boxes.concat();
    child_payload.extend_from_slice(&encode_typed_box(btrt, &[])?);
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn replace_visual_sample_entry_btrt(
    sample_entry_box: &[u8],
    btrt: &Btrt,
) -> Result<Vec<u8>, MuxError> {
    let stripped = strip_visual_sample_entry_immediate_children(
        sample_entry_box,
        &[FourCc::from_bytes(*b"btrt")],
    )?;
    append_visual_sample_entry_btrt(&stripped, btrt)
}

pub(crate) fn replace_visual_sample_entry_compressorname(
    sample_entry_box: &[u8],
    compressorname: [u8; 32],
) -> Result<Vec<u8>, MuxError> {
    let (mut sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    sample_entry.compressorname = compressorname;
    let mut child_payload = child_boxes.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn audio_sample_entry_immediate_children(
    sample_entry_box: &[u8],
) -> Result<Vec<Vec<u8>>, MuxError> {
    let (_, child_boxes, _) = decode_audio_sample_entry_parts(sample_entry_box)?;
    Ok(child_boxes)
}

pub(crate) fn visual_sample_entry_immediate_children(
    sample_entry_box: &[u8],
) -> Result<Vec<Vec<u8>>, MuxError> {
    let (_, child_boxes, _) = decode_visual_sample_entry_parts(sample_entry_box)?;
    Ok(child_boxes)
}

pub(crate) fn fragmented_visual_tkhd_dimensions_fixed_16_16(
    sample_entry_box: &[u8],
) -> Result<Option<(u32, u32)>, MuxError> {
    let (sample_entry, child_boxes, _) = decode_visual_sample_entry_parts(sample_entry_box)?;
    let Some(pasp_box) = child_boxes.iter().find(|child_box| {
        sample_entry_box_type(child_box).ok() == Some(FourCc::from_bytes(*b"pasp"))
    }) else {
        return Ok(None);
    };
    let pasp = decode_typed_box::<Pasp>(pasp_box)?;
    if pasp.h_spacing == 0 || pasp.v_spacing == 0 || (pasp.h_spacing == 1 && pasp.v_spacing == 1) {
        return Ok(None);
    }
    let numerator =
        u128::from(sample_entry.width) * u128::from(pasp.h_spacing) * u128::from(1_u32 << 16);
    let divisor = u128::from(pasp.v_spacing);
    let width_fixed_16_16 = numerator
        .saturating_add(divisor / 2)
        .checked_div(divisor)
        .unwrap_or(0);
    Ok(Some((
        u32::try_from(width_fixed_16_16)
            .map_err(|_| MuxError::LayoutOverflow("fragmented visual tkhd width"))?,
        u32::from(sample_entry.height) << 16,
    )))
}

fn is_square_pasp_box(child_box: &[u8]) -> Result<bool, MuxError> {
    let pasp = decode_typed_box::<Pasp>(child_box)?;
    Ok(pasp.h_spacing == 1 && pasp.v_spacing == 1)
}

pub(crate) fn replace_visual_sample_entry_immediate_children(
    sample_entry_box: &[u8],
    replacement_children: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, _, trailing_bytes) = decode_visual_sample_entry_parts(sample_entry_box)?;
    let mut child_payload = replacement_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn replace_audio_sample_entry_immediate_children(
    sample_entry_box: &[u8],
    replacement_children: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, _, trailing_bytes) = decode_audio_sample_entry_parts(sample_entry_box)?;
    let mut child_payload = replacement_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn replace_audio_sample_entry_immediate_children_without_trailing_bytes(
    sample_entry_box: &[u8],
    replacement_children: &[Vec<u8>],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, _, _) = decode_audio_sample_entry_parts(sample_entry_box)?;
    let child_payload = replacement_children.concat();
    encode_typed_box(&sample_entry, &child_payload)
}

pub(crate) fn strip_audio_sample_entry_immediate_children(
    sample_entry_box: &[u8],
    stripped_children: &[FourCc],
) -> Result<Vec<u8>, MuxError> {
    canonicalize_fragmented_audio_sample_entry_box(sample_entry_box, false, stripped_children)
}

pub(crate) fn strip_visual_sample_entry_immediate_children(
    sample_entry_box: &[u8],
    stripped_children: &[FourCc],
) -> Result<Vec<u8>, MuxError> {
    let (sample_entry, child_boxes, trailing_bytes) =
        decode_visual_sample_entry_parts(sample_entry_box)?;
    let mut normalized_children = Vec::with_capacity(child_boxes.len());
    for child_box in child_boxes {
        if stripped_children.contains(&sample_entry_box_type(&child_box)?) {
            continue;
        }
        normalized_children.push(child_box);
    }

    let mut child_payload = normalized_children.concat();
    child_payload.extend_from_slice(&trailing_bytes);
    encode_typed_box(&sample_entry, &child_payload)
}

fn canonicalize_fragmented_esds_box(esds_box: &[u8]) -> Result<Vec<u8>, MuxError> {
    let mut esds = decode_typed_box::<Esds>(esds_box)?;
    for descriptor in &mut esds.descriptors {
        if descriptor.tag == ES_DESCRIPTOR_TAG
            && let Some(es_descriptor) = descriptor.es_descriptor.as_mut()
        {
            es_descriptor.es_id = 0;
        }
    }
    esds.normalize_descriptor_sizes_for_mux()
        .map_err(|_| MuxError::LayoutOverflow("fragmented esds normalization"))?;
    encode_typed_box(&esds, &[])
}

pub(super) fn decode_visual_sample_entry_parts(
    sample_entry_box: &[u8],
) -> Result<SampleEntryParts<VisualSampleEntry>, MuxError> {
    let mut cursor = Cursor::new(sample_entry_box);
    let info = BoxInfo::read(&mut cursor)
        .map_err(|_| MuxError::LayoutOverflow("visual sample-entry header"))?;
    let mut sample_entry = VisualSampleEntry::default();
    sample_entry.sample_entry.box_type = info.box_type();
    unmarshal(
        &mut cursor,
        info.payload_size()
            .map_err(|_| MuxError::LayoutOverflow("visual sample-entry payload"))?,
        &mut sample_entry,
        None,
    )
    .map_err(|_| MuxError::LayoutOverflow("visual sample-entry decode"))?;
    split_box_children_and_trailing(sample_entry_box, cursor.position())
        .map(|(children, trailing)| (sample_entry, children, trailing))
}

pub(super) fn decode_audio_sample_entry_parts(
    sample_entry_box: &[u8],
) -> Result<SampleEntryParts<AudioSampleEntry>, MuxError> {
    let mut cursor = Cursor::new(sample_entry_box);
    let info = BoxInfo::read(&mut cursor)
        .map_err(|_| MuxError::LayoutOverflow("audio sample-entry header"))?;
    let mut sample_entry = AudioSampleEntry::default();
    sample_entry.sample_entry.box_type = info.box_type();
    unmarshal(
        &mut cursor,
        info.payload_size()
            .map_err(|_| MuxError::LayoutOverflow("audio sample-entry payload"))?,
        &mut sample_entry,
        None,
    )
    .map_err(|_| MuxError::LayoutOverflow("audio sample-entry decode"))?;
    split_box_children_and_trailing(sample_entry_box, cursor.position())
        .map(|(children, trailing)| (sample_entry, children, trailing))
}

pub(crate) fn decode_audio_sample_entry(
    sample_entry_box: &[u8],
) -> Result<AudioSampleEntry, MuxError> {
    let (sample_entry, _, _) = decode_audio_sample_entry_parts(sample_entry_box)?;
    Ok(sample_entry)
}

fn sample_entry_audio_sample_rate_int(sample_entry_box: &[u8]) -> Option<u32> {
    let (sample_entry, _, _) = decode_audio_sample_entry_parts(sample_entry_box).ok()?;
    Some(u32::from(sample_entry.sample_rate_int()))
}

pub(crate) fn decode_typed_box<B>(encoded_box: &[u8]) -> Result<B, MuxError>
where
    B: CodecBox + Default,
{
    let mut cursor = Cursor::new(encoded_box);
    let info =
        BoxInfo::read(&mut cursor).map_err(|_| MuxError::LayoutOverflow("typed box header"))?;
    let mut decoded = B::default();
    unmarshal(
        &mut cursor,
        info.payload_size()
            .map_err(|_| MuxError::LayoutOverflow("typed box payload"))?,
        &mut decoded,
        None,
    )
    .map_err(|_| MuxError::LayoutOverflow("typed box decode"))?;
    Ok(decoded)
}

fn split_box_children_and_trailing(
    sample_entry_box: &[u8],
    child_start: u64,
) -> Result<(SampleEntryChildBoxes, SampleEntryTrailingBytes), MuxError> {
    let child_start = usize::try_from(child_start)
        .map_err(|_| MuxError::LayoutOverflow("sample-entry child offset"))?;
    let remaining = sample_entry_box
        .get(child_start..)
        .ok_or(MuxError::LayoutOverflow("sample-entry child offset"))?;
    let child_bytes_len = split_box_children_with_optional_trailing_bytes(remaining);
    let child_boxes = split_immediate_box_bytes(&remaining[..child_bytes_len])?;
    Ok((child_boxes, remaining[child_bytes_len..].to_vec()))
}

fn split_immediate_box_bytes(bytes: &[u8]) -> Result<Vec<Vec<u8>>, MuxError> {
    let mut cursor = Cursor::new(bytes);
    let mut child_boxes = Vec::new();
    while cursor.position() < bytes.len() as u64 {
        let start = cursor.position();
        let info =
            BoxInfo::read(&mut cursor).map_err(|_| MuxError::LayoutOverflow("child box header"))?;
        let end = usize::try_from(
            start
                .checked_add(info.size())
                .ok_or(MuxError::LayoutOverflow("child box size"))?,
        )
        .map_err(|_| MuxError::LayoutOverflow("child box size"))?;
        child_boxes.push(bytes[start as usize..end].to_vec());
        cursor.set_position(end as u64);
    }
    Ok(child_boxes)
}

fn encode_compressor_name(name: &str) -> [u8; 32] {
    let mut encoded = [0_u8; 32];
    let visible = name.as_bytes();
    let visible_len = visible.len().min(31);
    encoded[0] = u8::try_from(visible_len).unwrap_or(31);
    encoded[1..1 + visible_len].copy_from_slice(&visible[..visible_len]);
    encoded
}

fn sample_entry_box_type(sample_entry_box: &[u8]) -> Result<FourCc, MuxError> {
    let mut cursor = Cursor::new(sample_entry_box);
    let info = BoxInfo::read(&mut cursor)
        .map_err(|_| MuxError::LayoutOverflow("sample-entry box header"))?;
    Ok(info.box_type())
}

fn encoded_box_type(box_bytes: &[u8]) -> Result<FourCc, MuxError> {
    let mut cursor = Cursor::new(box_bytes);
    let info = BoxInfo::read(&mut cursor).map_err(|_| MuxError::LayoutOverflow("box header"))?;
    Ok(info.box_type())
}

pub(crate) fn replace_opaque_text_sample_entry_btrt(
    sample_entry_box: &[u8],
    btrt: &Btrt,
) -> Result<Vec<u8>, MuxError> {
    let box_type = sample_entry_box_type(sample_entry_box)?;
    if box_type != FourCc::from_bytes(*b"text") && box_type != FourCc::from_bytes(*b"tx3g") {
        return Ok(sample_entry_box.to_vec());
    }
    if sample_entry_box.len() < 16 {
        return Ok(sample_entry_box.to_vec());
    }
    let payload = &sample_entry_box[8..];
    let Some(inline_child_start) = find_opaque_text_sample_entry_inline_child_start(payload) else {
        let mut payload = payload.to_vec();
        payload.extend_from_slice(&encode_typed_box(btrt, &[])?);
        return encode_raw_box(box_type, &payload);
    };

    let payload_prefix = &payload[..inline_child_start];
    let inline_suffix = &payload[inline_child_start..];
    let child_payload_len = split_box_children_with_optional_trailing_bytes(inline_suffix);
    let mut cursor = Cursor::new(&inline_suffix[..child_payload_len]);
    let mut normalized_inline_boxes = Vec::new();

    while usize::try_from(cursor.position()).unwrap_or(usize::MAX) < child_payload_len {
        let start = usize::try_from(cursor.position())
            .map_err(|_| MuxError::LayoutOverflow("opaque text child start"))?;
        let info = BoxInfo::read(&mut cursor)
            .map_err(|_| MuxError::LayoutOverflow("opaque text child header"))?;
        let end = start
            .checked_add(
                usize::try_from(info.size())
                    .map_err(|_| MuxError::LayoutOverflow("opaque text child size"))?,
            )
            .ok_or(MuxError::LayoutOverflow("opaque text child end"))?;
        if end > child_payload_len {
            return Err(MuxError::LayoutOverflow("opaque text child bounds"));
        }
        cursor.set_position(
            u64::try_from(end).map_err(|_| MuxError::LayoutOverflow("opaque text child seek"))?,
        );
        if info.box_type() == FourCc::from_bytes(*b"btrt") {
            continue;
        }
        normalized_inline_boxes.extend_from_slice(&inline_suffix[start..end]);
    }

    let mut payload = payload_prefix.to_vec();
    payload.extend_from_slice(&normalized_inline_boxes);
    payload.extend_from_slice(&encode_typed_box(btrt, &[])?);
    payload.extend_from_slice(&inline_suffix[child_payload_len..]);
    encode_raw_box(box_type, &payload)
}

fn find_opaque_text_sample_entry_inline_child_start(payload: &[u8]) -> Option<usize> {
    if payload.len() <= 8 {
        return None;
    }

    let opaque_payload = &payload[8..];
    for child_offset in 0..=opaque_payload.len().saturating_sub(8) {
        let suffix = &opaque_payload[child_offset..];
        let child_payload_len = split_box_children_with_optional_trailing_bytes(suffix);
        if child_payload_len == 0 {
            continue;
        }
        let Ok(first_child_type) = encoded_box_type(&suffix[..child_payload_len]) else {
            continue;
        };
        if first_child_type == FourCc::from_bytes(*b"ftab")
            || first_child_type == FourCc::from_bytes(*b"btrt")
        {
            return Some(8 + child_offset);
        }
    }

    None
}

fn copy_fragment_payloads<R, W>(
    sources: &mut [R],
    writer: &mut W,
    fragment: &FragmentLayout,
) -> Result<(), MuxError>
where
    R: Read + Seek,
    W: Write,
{
    let mut buffer = [0_u8; 16 * 1024];
    for sample in &fragment.samples {
        let source = sources
            .get_mut(sample.source_index)
            .ok_or(MuxError::LayoutOverflow("fragment source index"))?;
        source.seek(std::io::SeekFrom::Start(sample.source_data_offset))?;
        let mut remaining = sample.sample_size;
        while remaining > 0 {
            let chunk_len = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| MuxError::LayoutOverflow("fragment copy chunk"))?;
            source.read_exact(&mut buffer[..chunk_len])?;
            writer.write_all(&buffer[..chunk_len])?;
            remaining -= u64::try_from(chunk_len)
                .map_err(|_| MuxError::LayoutOverflow("fragment copy chunk"))?;
        }
    }
    Ok(())
}

#[cfg(feature = "async")]
async fn copy_fragment_payloads_async<R, W>(
    sources: &mut [R],
    writer: &mut W,
    fragment: &FragmentLayout,
) -> Result<(), MuxError>
where
    R: AsyncReadSeek,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; 16 * 1024];
    for sample in &fragment.samples {
        let source = sources
            .get_mut(sample.source_index)
            .ok_or(MuxError::LayoutOverflow("fragment source index"))?;
        source
            .seek(std::io::SeekFrom::Start(sample.source_data_offset))
            .await?;
        let mut remaining = sample.sample_size;
        while remaining > 0 {
            let chunk_len = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| MuxError::LayoutOverflow("fragment copy chunk"))?;
            source.read_exact(&mut buffer[..chunk_len]).await?;
            writer.write_all(&buffer[..chunk_len]).await?;
            remaining -= u64::try_from(chunk_len)
                .map_err(|_| MuxError::LayoutOverflow("fragment copy chunk"))?;
        }
    }
    Ok(())
}

fn sample_flags(
    sample: &PreparedSample,
    sample_index: usize,
    first_sync_sample_index: Option<usize>,
) -> u32 {
    let is_sync_sample = first_sync_sample_index
        .map_or(sample.is_sync_sample, |first_sync_index| {
            sample.is_sync_sample && sample_index == first_sync_index
        });
    if is_sync_sample {
        0
    } else {
        NON_KEY_SAMPLE_FLAGS
    }
}

fn fragmented_track_emits_roll_description(track: &PreparedTrack<'_>) -> bool {
    let Some(sample_roll_distance) = track.config.sample_roll_distance() else {
        return false;
    };
    if !sample_entry_matches(track.sample_entry_box, &[b"Opus"]) {
        return true;
    }
    sample_roll_distance < 0
}

fn fragmented_track_emits_roll_assignment(track: &PreparedTrack<'_>) -> bool {
    if !track.config.emit_roll_sbgp() {
        return false;
    }
    if !sample_entry_matches(track.sample_entry_box, &[b"Opus"]) {
        return true;
    }
    track
        .config
        .sample_roll_distance()
        .is_some_and(|sample_roll_distance| sample_roll_distance < 0)
}

fn fragmented_track_uses_trimmed_non_square_avc_pasp(
    track: &PreparedTrack<'_>,
) -> Result<bool, MuxError> {
    if track.config.kind() != MuxTrackKind::Video
        || track.config.edit_media_time().is_none()
        || !sample_entry_matches(track.sample_entry_box, &[b"avc1"])
    {
        return Ok(false);
    }
    let child_boxes = visual_sample_entry_immediate_children(track.sample_entry_box)?;
    for child_box in child_boxes {
        if sample_entry_box_type(&child_box)? != FourCc::from_bytes(*b"pasp") {
            continue;
        }
        let pasp = decode_typed_box::<Pasp>(&child_box)?;
        return Ok(pasp.h_spacing != 0 && pasp.h_spacing != pasp.v_spacing);
    }
    Ok(false)
}

fn sample_entry_carries_child_type(sample_entry_box: &[u8], child_types: &[FourCc]) -> bool {
    visual_sample_entry_immediate_children(sample_entry_box).is_ok_and(|child_boxes| {
        child_boxes.iter().any(|child_box| {
            sample_entry_box_type(child_box)
                .ok()
                .is_some_and(|child_type| child_types.contains(&child_type))
        })
    })
}

fn all_equal_u32<I>(mut values: I) -> Option<u32>
where
    I: Iterator<Item = u32>,
{
    let first = values.next()?;
    values.all(|value| value == first).then_some(first)
}

fn all_equal_u64<I>(mut values: I) -> Option<u64>
where
    I: Iterator<Item = u64>,
{
    let first = values.next()?;
    values.all(|value| value == first).then_some(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boxes::iso14496_12::AVCDecoderConfiguration;
    use crate::mux::StscRunEncodingMode;
    use crate::mux::{
        FlatTimingOverride, MuxInterleavePolicy, MuxStagedMediaItem,
        plan_staged_media_items_with_chunk_sample_counts,
    };

    fn test_prepared_sample(
        decode_time_movie: u64,
        duration_movie: u32,
        is_sync_sample: bool,
    ) -> PreparedSample {
        test_prepared_sample_with_size(decode_time_movie, duration_movie, 0, is_sync_sample)
    }

    fn test_prepared_sample_with_size(
        decode_time_movie: u64,
        duration_movie: u32,
        sample_size: u64,
        is_sync_sample: bool,
    ) -> PreparedSample {
        PreparedSample {
            source_index: 0,
            source_data_offset: 0,
            decode_time_movie,
            decode_time_media: 0,
            output_offset: 0,
            sample_size,
            duration_movie,
            duration_media: 0,
            composition_offset_movie: 0,
            composition_offset_media: 0,
            is_sync_sample,
            sample_description_index: 1,
        }
    }

    #[test]
    fn build_fragmented_tkhd_uses_default_reference_flags() {
        let config = MuxTrackConfig::new_audio(1, 48_000, Vec::new()).with_tkhd_flags(0x000f);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &[],
            samples: Vec::new(),
            chunk_sample_counts: Vec::new(),
            fragmented_reference_group_fragment_counts: None,
            media_duration: 0,
            presentation_duration_media: 0,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let tkhd = build_fragmented_tkhd(&track, 123).expect("fragmented tkhd");

        assert_eq!(tkhd.flags(), DEFAULT_FRAGMENTED_TKHD_FLAGS);
    }

    #[test]
    fn build_fragmented_tkhd_resets_audio_volume_and_matrix() {
        let config = MuxTrackConfig::new_audio(1, 48_000, Vec::new())
            .with_volume(0)
            .with_matrix([0; 9])
            .with_alternate_group(7);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &[],
            samples: Vec::new(),
            chunk_sample_counts: Vec::new(),
            fragmented_reference_group_fragment_counts: None,
            media_duration: 0,
            presentation_duration_media: 0,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let tkhd = build_fragmented_tkhd(&track, 123).expect("fragmented tkhd");

        assert_eq!(tkhd.alternate_group, 0);
        assert_eq!(tkhd.volume, 0x0100);
        assert_eq!(tkhd.matrix, IDENTITY_MATRIX);
    }

    #[test]
    fn build_sidx_reference_tracks_delayed_first_sap_after_trim() {
        let mut samples = Vec::new();
        for sample_index in 0..26_u64 {
            samples.push(test_prepared_sample(
                sample_index * 1024,
                1024,
                sample_index == 25,
            ));
        }
        let fragment = FragmentLayout {
            segment_type_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
            moof_bytes: Vec::new(),
            mdat_header: Vec::new(),
            samples: samples.clone(),
            sidx_samples: samples,
        };

        let built =
            build_sidx_reference(std::iter::once(&fragment), 3_072, true).expect("sidx reference");

        assert!(!built.reference.starts_with_sap);
        assert_eq!(built.reference.sap_type, 1);
        assert_eq!(built.reference.sap_delta_time, 22_528);
        assert_eq!(built.earliest_presentation_time, 0);
        assert_eq!(built.reference.subsegment_duration, 23_552);
    }

    fn immediate_child_types(encoded_box: &[u8]) -> Vec<FourCc> {
        let mut cursor = Cursor::new(encoded_box);
        let parent = BoxInfo::read(&mut cursor).expect("parent header");
        let mut child_types = Vec::new();
        std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(parent.header_size())).unwrap();
        while cursor.position() < parent.size() {
            let child = BoxInfo::read(&mut cursor).expect("child header");
            child_types.push(child.box_type());
            child.seek_to_end(&mut cursor).expect("child seek");
        }
        child_types
    }

    #[test]
    fn build_trak_bytes_places_preserved_tref_before_mdia() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("sample entry");
        let tref_child =
            encode_raw_box(FourCc::from_bytes(*b"sync"), &1_u32.to_be_bytes()).expect("tref child");
        let tref = encode_raw_box(FourCc::from_bytes(*b"tref"), &tref_child).expect("tref");
        let udta = encode_raw_box(FourCc::from_bytes(*b"udta"), b"user").expect("udta");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_preserved_flat_trak_boxes(vec![tref, udta]);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample_with_size(0, 1_024, 4, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let trak =
            build_trak_bytes(&MuxFileConfig::new(48_000), &track, 24, 8, 256, None).expect("trak");

        assert_eq!(
            immediate_child_types(&trak),
            vec![
                FourCc::from_bytes(*b"tkhd"),
                FourCc::from_bytes(*b"tref"),
                FourCc::from_bytes(*b"mdia"),
                FourCc::from_bytes(*b"udta"),
            ]
        );
    }

    fn test_visual_sample_entry_box(box_type: FourCc) -> Vec<u8> {
        encode_typed_box(
            &VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type,
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..VisualSampleEntry::default()
            },
            &[],
        )
        .expect("visual sample entry")
    }

    fn test_basic_sample_entry_box(box_type: FourCc) -> Vec<u8> {
        encode_raw_box(box_type, &[0, 0, 0, 0, 0, 0, 0, 1]).expect("sample entry")
    }

    fn test_prepared_track<'a>(
        config: &'a MuxTrackConfig,
        sample_entry_box: &'a [u8],
        duration: u32,
    ) -> PreparedTrack<'a> {
        PreparedTrack {
            config,
            sample_entry_box,
            samples: vec![test_prepared_sample(0, duration, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: u64::from(duration),
            presentation_duration_media: u64::from(duration),
            edit_media_time: None,
            flat_timing_override: None,
        }
    }

    #[test]
    fn infer_auto_flat_ftyp_profile_keeps_avc_brand_with_hevc_video() {
        let avc_entry = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let avc_config = MuxTrackConfig::new_video(1, 1_000, 640, 360, avc_entry.clone());
        let avc_track = test_prepared_track(&avc_config, &avc_entry, 1_000);
        let hevc_entry = test_visual_sample_entry_box(FourCc::from_bytes(*b"hvc1"));
        let hevc_config = MuxTrackConfig::new_video(2, 1_000, 640, 360, hevc_entry.clone());
        let hevc_track = test_prepared_track(&hevc_config, &hevc_entry, 1_000);

        let (major_brand, minor_version, compatible_brands) =
            infer_auto_flat_ftyp_profile(&[avc_track, hevc_track]);

        assert_eq!(major_brand, FourCc::from_bytes(*b"iso4"));
        assert_eq!(minor_version, 1);
        assert_eq!(
            compatible_brands,
            vec![FourCc::from_bytes(*b"avc1"), FourCc::from_bytes(*b"iso4")]
        );
    }

    #[test]
    fn infer_auto_flat_ftyp_profile_keeps_avc_brand_with_avs3_video() {
        let avc_entry = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let avc_config = MuxTrackConfig::new_video(1, 1_000, 640, 360, avc_entry.clone());
        let avc_track = test_prepared_track(&avc_config, &avc_entry, 1_000);
        let avs3_entry = test_visual_sample_entry_box(FourCc::from_bytes(*b"avs3"));
        let avs3_config = MuxTrackConfig::new_video(2, 1_000, 640, 360, avs3_entry.clone());
        let avs3_track = test_prepared_track(&avs3_config, &avs3_entry, 1_000);

        let (major_brand, minor_version, compatible_brands) =
            infer_auto_flat_ftyp_profile(&[avc_track, avs3_track]);

        assert_eq!(major_brand, FourCc::from_bytes(*b"iso4"));
        assert_eq!(minor_version, 1);
        assert_eq!(
            compatible_brands,
            vec![
                FourCc::from_bytes(*b"avc1"),
                FourCc::from_bytes(*b"iso4"),
                FourCc::from_bytes(*b"cav3"),
            ]
        );
    }

    #[test]
    fn build_flat_iods_bytes_treats_avc3_as_avc() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc3"));
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0x7f);
    }

    #[test]
    fn build_flat_iods_bytes_uses_simple_visual_profile_for_avc_plus_timed_text() {
        let video_sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let video_config =
            MuxTrackConfig::new_video(1, 1_000, 640, 360, video_sample_entry_box.clone());
        let video_track = test_prepared_track(&video_config, &video_sample_entry_box, 1_000);
        let text_sample_entry_box = test_basic_sample_entry_box(FourCc::from_bytes(*b"stpp"));
        let text_config =
            MuxTrackConfig::new_subtitle(2, 1_000, 640, 360, text_sample_entry_box.clone());
        let text_track = test_prepared_track(&text_config, &text_sample_entry_box, 1_000);
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[video_track, text_track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0x15);
    }

    #[test]
    fn build_flat_iods_bytes_keeps_avc_visual_profile_for_mp4s_subtitle() {
        let video_sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let video_config =
            MuxTrackConfig::new_video(1, 1_000, 640, 360, video_sample_entry_box.clone());
        let video_track = test_prepared_track(&video_config, &video_sample_entry_box, 1_000);
        let subtitle_sample_entry_box = test_basic_sample_entry_box(FourCc::from_bytes(*b"mp4s"));
        let subtitle_config =
            MuxTrackConfig::new_subtitle(2, 1_000, 640, 360, subtitle_sample_entry_box.clone());
        let subtitle_track =
            test_prepared_track(&subtitle_config, &subtitle_sample_entry_box, 1_000);
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[video_track, subtitle_track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0x7f);
    }

    #[test]
    fn build_flat_iods_bytes_uses_unknown_visual_profile_for_imported_authority_avc_mp4a_tracks() {
        let video_sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let video_config =
            MuxTrackConfig::new_video(1, 1_000, 640, 360, video_sample_entry_box.clone())
                .with_flat_source_track_creation_time(Some(1))
                .with_flat_source_media_creation_time(Some(1));
        let video_track = PreparedTrack {
            config: &video_config,
            sample_entry_box: &video_sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let audio_sample_entry_box = encode_typed_box(
            &AudioSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..AudioSampleEntry::default()
            },
            &[],
        )
        .expect("mp4a sample entry");
        let audio_config = MuxTrackConfig::new_audio(2, 48_000, audio_sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let audio_track = PreparedTrack {
            config: &audio_config,
            sample_entry_box: &audio_sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[video_track, audio_track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0xff);
    }

    #[test]
    fn build_flat_iods_bytes_omits_direct_vvc1_tracks() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"vvc1"));
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        assert!(
            build_flat_iods_bytes(&file_config, &[track])
                .expect("flat iods")
                .is_none()
        );
    }

    #[test]
    fn build_flat_iods_bytes_uses_no_visual_profile_for_vvc_mp4a_tracks() {
        let video_sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"vvc1"));
        let video_config =
            MuxTrackConfig::new_video(1, 90_000, 640, 360, video_sample_entry_box.clone());
        let video_track = test_prepared_track(&video_config, &video_sample_entry_box, 0);
        let audio_sample_entry_box = encode_typed_box(
            &AudioSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..AudioSampleEntry::default()
            },
            &[],
        )
        .expect("mp4a sample entry");
        let audio_config = MuxTrackConfig::new_audio(2, 48_000, audio_sample_entry_box.clone());
        let audio_track = test_prepared_track(&audio_config, &audio_sample_entry_box, 1_024);
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[video_track, audio_track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0xff);
    }

    #[test]
    fn build_flat_iods_bytes_omits_imported_authority_vorbis_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 48_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0xDD,
                stream_type: 5,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..Descriptor::default()
        }];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(48_000).with_auto_flat_profile(true);

        assert!(
            build_flat_iods_bytes(&file_config, &[track])
                .expect("flat iods")
                .is_none()
        );
    }

    #[test]
    fn build_flat_iods_bytes_omits_imported_authority_voice_mp4a_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 8_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0xE1,
                stream_type: 5,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..Descriptor::default()
        }];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 8_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 160, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 160,
            presentation_duration_media: 160,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(8_000)
            .with_auto_flat_profile(true)
            .with_allow_audio_only_iods(true);

        assert!(
            build_flat_iods_bytes(&file_config, &[track])
                .expect("flat iods")
                .is_none()
        );
    }

    #[test]
    fn build_flat_iods_bytes_omits_imported_authority_direct_voice_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"sqcp"),
                data_reference_index: 1,
            },
            channel_count: 1,
            sample_size: 16,
            sample_rate: 8_000 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box = encode_typed_box(&sample_entry, &[]).expect("sqcp sample entry");
        let config = MuxTrackConfig::new_audio(1, 8_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 160, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 160,
            presentation_duration_media: 160,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(8_000)
            .with_auto_flat_profile(true)
            .with_allow_audio_only_iods(true);

        assert!(
            build_flat_iods_bytes(&file_config, &[track])
                .expect("flat iods")
                .is_none()
        );
    }

    #[test]
    fn build_flat_iods_bytes_omits_imported_authority_speex_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"spex"),
                data_reference_index: 1,
            },
            channel_count: 1,
            sample_size: 16,
            sample_rate: 16_000 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box = encode_typed_box(&sample_entry, &[]).expect("spex sample entry");
        let config = MuxTrackConfig::new_audio(1, 16_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1))
            .with_omit_flat_iods(true);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 320, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 320,
            presentation_duration_media: 320,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(16_000)
            .with_auto_flat_profile(true)
            .with_allow_audio_only_iods(true);

        assert!(
            build_flat_iods_bytes(&file_config, &[track])
                .expect("flat iods")
                .is_none()
        );
    }

    #[test]
    fn build_flat_iods_bytes_authors_direct_speex_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"spex"),
                data_reference_index: 1,
            },
            channel_count: 1,
            sample_size: 16,
            sample_rate: 16_000 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box = encode_typed_box(&sample_entry, &[]).expect("spex sample entry");
        let config = MuxTrackConfig::new_audio(1, 16_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 320, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 320,
            presentation_duration_media: 320,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(16_000)
            .with_auto_flat_profile(true)
            .with_allow_audio_only_iods(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");
        assert_eq!(descriptor.audio_profile_level_indication, 0xfe);
        assert_eq!(descriptor.visual_profile_level_indication, 0xff);
    }

    #[test]
    fn build_flat_udta_bytes_keeps_tool_metadata_for_imported_authority_speex_only_tracks() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"spex"),
                data_reference_index: 1,
            },
            channel_count: 1,
            sample_size: 16,
            sample_rate: 16_000 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box = encode_typed_box(&sample_entry, &[]).expect("spex sample entry");
        let config = MuxTrackConfig::new_audio(1, 16_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 320, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 320,
            presentation_duration_media: 320,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(16_000).with_auto_flat_profile(true);

        assert!(
            build_flat_udta_bytes(&file_config, &[track])
                .expect("flat udta")
                .is_some()
        );
    }

    #[test]
    fn build_flat_iods_bytes_uses_he_aac_v2_audio_profile_level() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 48_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication: 0x40,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..Descriptor::default()
            },
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: 9,
                data: vec![0x10, 0x02, 0xb7, 0x2f, 0xc0, 0x00, 0x00, 0x2a, 0x44],
                ..Descriptor::default()
            },
        ];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(48_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.audio_profile_level_indication, 0x2c);
    }

    #[test]
    fn build_flat_iods_bytes_uses_xhe_aac_audio_profile_level() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 48_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication: 0x40,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..Descriptor::default()
            },
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: 3,
                data: vec![0xF9, 0x46, 0x40],
                ..Descriptor::default()
            },
        ];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 2_048, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 2_048,
            presentation_duration_media: 2_048,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(48_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.audio_profile_level_indication, 0x0f);
    }

    #[test]
    fn build_flat_iods_bytes_uses_he_aac_audio_profile_level_for_low_rate_sbr() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 24_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication: 0x40,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..Descriptor::default()
            },
            Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: 4,
                data: vec![0x2b, 0x92, 0x08, 0x00],
                ..Descriptor::default()
            },
        ];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 24_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(24_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.audio_profile_level_indication, 0x28);
    }

    #[test]
    fn build_flat_iods_bytes_uses_unknown_audio_profile_for_imported_authority_mp3_mp4a() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 48_000 << 16,
            ..AudioSampleEntry::default()
        };
        let mut esds = Esds::default();
        esds.descriptors = vec![Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0x6b,
                stream_type: 5,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..Descriptor::default()
        }];
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_152, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_152,
            presentation_duration_media: 1_152,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(48_000)
            .with_auto_flat_profile(true)
            .with_allow_audio_only_iods(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.audio_profile_level_indication, 0xfe);
    }

    #[test]
    fn build_flat_iods_bytes_uses_configured_mhm1_audio_profile_level() {
        let btrt_bytes = encode_typed_box(&Btrt::default(), &[]).expect("btrt");
        let sample_entry_box = encode_typed_box(
            &AudioSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"mhm1"),
                    data_reference_index: 1,
                },
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..AudioSampleEntry::default()
            },
            &btrt_bytes,
        )
        .expect("mhm1 sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_flat_audio_profile_level_indication(0x0e);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(48_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.audio_profile_level_indication, 0x0e);
    }

    #[test]
    fn build_flat_iods_bytes_uses_unknown_visual_profile_for_imported_authority_mpeg2_mp4v() {
        let mut esds = Esds::default();
        esds.descriptors = vec![Descriptor {
            tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
            decoder_config_descriptor: Some(crate::boxes::iso14496_14::DecoderConfigDescriptor {
                object_type_indication: 0x60,
                stream_type: 4,
                reserved: true,
                ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
            }),
            ..Descriptor::default()
        }];
        let sample_entry_box = encode_typed_box(
            &VisualSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4v"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..VisualSampleEntry::default()
            },
            &encode_typed_box(&esds, &[]).expect("esds"),
        )
        .expect("mp4v sample entry");
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1))
            .with_flat_source_media_creation_time(Some(1));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let iods_bytes = build_flat_iods_bytes(&file_config, &[track])
            .expect("flat iods")
            .expect("present iods");
        let iods = decode_typed_box::<Iods>(&iods_bytes).expect("decode iods");
        let descriptor = iods
            .initial_object_descriptor()
            .expect("initial descriptor");

        assert_eq!(descriptor.visual_profile_level_indication, 0xfe);
    }

    #[test]
    fn fragmented_visual_tkhd_dimensions_fixed_16_16_preserves_non_square_pasp_width() {
        let avcc = encode_typed_box(
            &AVCDecoderConfiguration {
                configuration_version: 1,
                profile: 100,
                profile_compatibility: 0,
                level: 31,
                length_size_minus_one: 3,
                ..AVCDecoderConfiguration::default()
            },
            &[],
        )
        .expect("avcc");
        let pasp = encode_typed_box(
            &Pasp {
                h_spacing: 8,
                v_spacing: 9,
            },
            &[],
        )
        .expect("pasp");
        let sample_entry = encode_typed_box(
            &VisualSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"avc1"),
                    data_reference_index: 1,
                },
                width: 706,
                height: 472,
                ..VisualSampleEntry::default()
            },
            &[avcc, pasp].concat(),
        )
        .expect("sample entry");

        let dimensions =
            fragmented_visual_tkhd_dimensions_fixed_16_16(&sample_entry).expect("dimensions");

        assert_eq!(dimensions, Some((41_127_481, 30_932_992)));
    }

    #[test]
    fn build_mdhd_preserves_imported_authority_video_media_modification_time() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(0))
            .with_flat_source_media_creation_time(Some(0))
            .with_flat_source_media_modification_time(Some(0));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let mdhd = build_mdhd(&track, Some(123)).expect("mdhd");

        assert_eq!(mdhd.creation_time(), 0);
        assert_eq!(mdhd.modification_time(), 0);
    }

    #[test]
    fn build_tkhd_uses_generated_modification_time_for_imported_authority_video_when_source_value_is_zero()
     {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc1"));
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(0))
            .with_flat_source_track_modification_time(Some(0))
            .with_flat_source_media_creation_time(Some(0));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };
        let file_config = MuxFileConfig::new(1_000).with_auto_flat_profile(true);

        let tkhd = build_tkhd(&file_config, &track, Some(123)).expect("tkhd");

        assert_eq!(tkhd.creation_time(), 0);
        assert_eq!(tkhd.modification_time(), 123);
    }

    #[test]
    fn build_mdhd_keeps_generated_media_modification_time_for_imported_authority_audio() {
        let sample_entry_box = encode_typed_box(
            &AudioSampleEntry {
                sample_entry: SampleEntry {
                    box_type: FourCc::from_bytes(*b"mp4a"),
                    data_reference_index: 1,
                },
                channel_count: 2,
                sample_size: 16,
                sample_rate: 48_000 << 16,
                ..AudioSampleEntry::default()
            },
            &[],
        )
        .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(0))
            .with_flat_source_media_creation_time(Some(0))
            .with_flat_source_media_modification_time(Some(0));
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_024, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_024,
            presentation_duration_media: 1_024,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let mdhd = build_mdhd(&track, Some(123)).expect("mdhd");

        assert_eq!(mdhd.creation_time(), 0);
        assert_eq!(mdhd.modification_time(), 123);
    }

    #[test]
    fn build_fragmented_ftyp_bytes_uses_avc3_brand_without_cmfc() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc3"));
        let config = MuxTrackConfig::new_video(1, 1_000, 640, 360, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 1_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_000,
            presentation_duration_media: 1_000,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let ftyp = build_fragmented_ftyp(&[track]).expect("fragmented ftyp");
        let ftyp_bytes = encode_typed_box(&ftyp, &[]).expect("fragmented ftyp bytes");
        let ftyp = decode_typed_box::<Ftyp>(&ftyp_bytes).expect("decode ftyp");

        assert_eq!(ftyp.major_brand, FourCc::from_bytes(*b"mp41"));
        assert_eq!(
            ftyp.compatible_brands,
            vec![
                FourCc::from_bytes(*b"iso8"),
                FourCc::from_bytes(*b"isom"),
                FourCc::from_bytes(*b"mp41"),
                FourCc::from_bytes(*b"dash"),
                FourCc::from_bytes(*b"avc3"),
            ]
        );
    }

    #[test]
    fn canonicalize_fragmented_sample_entry_box_sets_avc3_compressor_name() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"avc3"));

        let normalized =
            canonicalize_fragmented_sample_entry_box(&sample_entry_box).expect("normalize avc3");
        let (sample_entry, _, _) =
            decode_visual_sample_entry_parts(&normalized).expect("decode visual sample entry");
        let visible_len = usize::from(sample_entry.compressorname[0]).min(31);

        assert_eq!(
            &sample_entry.compressorname[1..1 + visible_len],
            b"AVC Coding"
        );
    }

    #[test]
    fn canonicalize_fragmented_sample_entry_box_reorders_extended_av1_children() {
        let child_payload = [
            encode_raw_box(FourCc::from_bytes(*b"av1C"), &[0]).expect("av1C"),
            encode_raw_box(FourCc::from_bytes(*b"colr"), &[0]).expect("colr"),
            encode_raw_box(FourCc::from_bytes(*b"btrt"), &[0]).expect("btrt"),
            encode_raw_box(FourCc::from_bytes(*b"dvvC"), &[0]).expect("dvvC"),
        ]
        .concat();
        let sample_entry_box = encode_typed_box(
            &VisualSampleEntry {
                sample_entry: crate::boxes::iso14496_12::SampleEntry {
                    box_type: FourCc::from_bytes(*b"av01"),
                    data_reference_index: 1,
                },
                width: 640,
                height: 360,
                ..VisualSampleEntry::default()
            },
            &child_payload,
        )
        .expect("av01 sample entry");

        let normalized =
            canonicalize_fragmented_sample_entry_box(&sample_entry_box).expect("normalize av01");
        let child_types = visual_sample_entry_immediate_children(&normalized)
            .expect("visual children")
            .into_iter()
            .map(|child_box| sample_entry_box_type(&child_box).expect("child type"))
            .collect::<Vec<_>>();

        assert_eq!(
            child_types,
            vec![
                FourCc::from_bytes(*b"av1C"),
                FourCc::from_bytes(*b"dvvC"),
                FourCc::from_bytes(*b"colr"),
            ]
        );
    }

    #[test]
    fn fragmented_mehd_duration_trims_vp08_presentation_span_by_one_tick() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"vp08"));
        let config = MuxTrackConfig::new_video(1, 30_000, 640, 360, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 259_999, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 259_999,
            presentation_duration_media: 260_000,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(30_000, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 259_998);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_full_imported_vp08_presentation_span() {
        let sample_entry_box = test_visual_sample_entry_box(FourCc::from_bytes(*b"vp08"));
        let config = MuxTrackConfig::new_video(1, 1_000_000, 640, 360, sample_entry_box.clone())
            .with_stsc_run_encoding_mode(StscRunEncodingMode::PreserveTerminalBoundary);
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![test_prepared_sample(0, 2_736_000, true)],
            chunk_sample_counts: vec![1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 2_736_000,
            presentation_duration_media: 2_736_000,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration =
            fragmented_mehd_duration(1_000_000, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 2_736_000);
    }

    #[test]
    fn preserved_flat_stsc_override_keeps_explicit_duplicate_boundaries() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let mut preserved_stsc = Stsc::default();
        preserved_stsc.entry_count = 3;
        preserved_stsc.entries = vec![
            StscEntry {
                first_chunk: 1,
                samples_per_chunk: 2,
                sample_description_index: 1,
            },
            StscEntry {
                first_chunk: 2,
                samples_per_chunk: 2,
                sample_description_index: 1,
            },
            StscEntry {
                first_chunk: 3,
                samples_per_chunk: 1,
                sample_description_index: 1,
            },
        ];
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone())
            .with_flat_stsc_override(preserved_stsc.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_024, true),
                test_prepared_sample(1_024, 1_024, true),
                test_prepared_sample(2_048, 1_024, true),
                test_prepared_sample(3_072, 1_024, true),
                test_prepared_sample(4_096, 1_024, true),
            ],
            chunk_sample_counts: vec![2, 2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 5_120,
            presentation_duration_media: 5_120,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let stsc = preserved_flat_stsc_or_built(&track).expect("preserved stsc");

        assert_eq!(stsc, preserved_stsc);
    }

    #[test]
    fn fragmented_mehd_duration_uses_audio_sample_span_when_media_duration_rounds_up() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 264_192, true),
                test_prepared_sample(264_192, 264_192, true),
                test_prepared_sample(528_384, 120_832, true),
            ],
            chunk_sample_counts: vec![2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 649_217,
            presentation_duration_media: 649_217,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 649_216);
    }

    #[test]
    fn limit_sidx_references_caps_to_encoded_field_capacity() {
        let references = (0..=MAX_SIDX_REFERENCES)
            .map(|index| BuiltSidxReference {
                reference: SidxReference {
                    referenced_size: 1,
                    subsegment_duration: 1,
                    starts_with_sap: true,
                    ..SidxReference::default()
                },
                earliest_presentation_time: u64::try_from(index).unwrap(),
            })
            .collect::<Vec<_>>();

        let limited = limit_sidx_references(references);

        assert_eq!(limited.len(), MAX_SIDX_REFERENCES);
        assert_eq!(
            limited.last().unwrap().earliest_presentation_time,
            u64::from(u16::MAX - 1)
        );
    }

    #[test]
    fn prepare_track_uses_flat_timing_override_decode_times_for_rounded_movie_ticks() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(2, 44_100, sample_entry_box)
            .with_flat_timing_override(FlatTimingOverride {
                sample_durations: vec![1_024, 1_024],
                composition_offsets: vec![0, 0],
                media_duration: 2_048,
                presentation_duration: 2_048,
            });
        let plan = plan_staged_media_items_with_chunk_sample_counts(
            vec![
                MuxStagedMediaItem::new(0, 2, 0, 23, 0, 4),
                MuxStagedMediaItem::new(0, 2, 23, 23, 4, 4),
            ],
            MuxInterleavePolicy::DecodeTime,
            [(2, vec![2])],
        )
        .expect("plan");

        let prepared = prepare_track(
            &MuxFileConfig::new(1_000),
            &plan,
            &config,
            plan.planned_items().iter().collect(),
            true,
        )
        .expect("prepared track");

        assert_eq!(
            prepared
                .samples
                .iter()
                .map(|sample| sample.decode_time_media)
                .collect::<Vec<_>>(),
            vec![0, 1_024]
        );
    }

    #[test]
    fn fragmented_mehd_duration_floors_imported_audio_authority_duration_when_movie_timescale_differs()
     {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 10, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1));
        let override_value = FlatTimingOverride {
            sample_durations: vec![3, 3, 3],
            composition_offsets: vec![0, 0, 0],
            media_duration: 9,
            presentation_duration: 9,
        };
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1, true),
                test_prepared_sample(1, 1, true),
                test_prepared_sample(2, 1, true),
            ],
            chunk_sample_counts: vec![2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 9,
            presentation_duration_media: 9,
            edit_media_time: None,
            flat_timing_override: Some(&override_value),
        };

        let duration = fragmented_mehd_duration(4, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 3);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_imported_audio_authority_media_duration_at_same_timescale()
     {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1));
        let override_value = FlatTimingOverride {
            sample_durations: vec![1_024, 1_024, 1_024],
            composition_offsets: vec![0, 0, 0],
            media_duration: 3_072,
            presentation_duration: 3_071,
        };
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_024, true),
                test_prepared_sample(1_024, 1_024, true),
                test_prepared_sample(2_048, 1_024, true),
            ],
            chunk_sample_counts: vec![2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 3_072,
            presentation_duration_media: 3_071,
            edit_media_time: None,
            flat_timing_override: Some(&override_value),
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 3_072);
    }

    #[test]
    fn fragmented_mehd_duration_uses_imported_audio_sample_span_when_authority_media_duration_is_one_tick_larger()
     {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1));
        let override_value = FlatTimingOverride {
            sample_durations: vec![1_024, 1_024, 1_024],
            composition_offsets: vec![0, 0, 0],
            media_duration: 3_072,
            presentation_duration: 3_071,
        };
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_024, true),
                test_prepared_sample(1_024, 1_024, true),
                test_prepared_sample(2_048, 1_023, true),
            ],
            chunk_sample_counts: vec![2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 3_072,
            presentation_duration_media: 3_071,
            edit_media_time: None,
            flat_timing_override: Some(&override_value),
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 3_071);
    }

    #[test]
    fn fragmented_mehd_duration_scales_imported_audio_authority_media_duration_when_movie_timescale_differs()
     {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 10, sample_entry_box.clone())
            .with_flat_source_track_creation_time(Some(1));
        let override_value = FlatTimingOverride {
            sample_durations: vec![3, 3, 3],
            composition_offsets: vec![0, 0, 0],
            media_duration: 8,
            presentation_duration: 9,
        };
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1, true),
                test_prepared_sample(1, 1, true),
                test_prepared_sample(2, 1, true),
            ],
            chunk_sample_counts: vec![2, 1],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 8,
            presentation_duration_media: 9,
            edit_media_time: None,
            flat_timing_override: Some(&override_value),
        };

        let duration = fragmented_mehd_duration(4, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 3);
    }

    #[test]
    fn fragmented_mehd_duration_trims_even_full_frame_mp4a_by_one_tick() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_024, true),
                test_prepared_sample(1_024, 1_024, true),
            ],
            chunk_sample_counts: vec![2],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 2_048,
            presentation_duration_media: 2_048,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 2_047);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_terminal_short_frame_mp4a() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"mp4a"), &[]).expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_024, true),
                test_prepared_sample(1_024, 720, true),
            ],
            chunk_sample_counts: vec![2],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 1_744,
            presentation_duration_media: 1_744,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 1_744);
    }

    #[test]
    fn fragmented_mehd_duration_trims_odd_ec3_sample_count_by_one_tick() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"ec-3"), &[]).expect("ec-3 sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_536, true),
                test_prepared_sample(1_536, 1_536, true),
                test_prepared_sample(3_072, 1_536, true),
            ],
            chunk_sample_counts: vec![3],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 4_608,
            presentation_duration_media: 4_608,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(48_000, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 4_607);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_even_ec3_sample_count() {
        let sample_entry_box =
            encode_raw_box(FourCc::from_bytes(*b"ec-3"), &[]).expect("ec-3 sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_536, true),
                test_prepared_sample(1_536, 1_536, true),
            ],
            chunk_sample_counts: vec![2],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 3_072,
            presentation_duration_media: 3_072,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(48_000, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 3_072);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_odd_44100_ec3_sample_count() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"ec-3"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 44_100 << 16,
            ..AudioSampleEntry::default()
        };
        let dec3 = Dec3 {
            data_rate: 192,
            num_ind_sub: 0,
            ec3_substreams: vec![crate::boxes::etsi_ts_102_366::Ec3Substream::default()],
            reserved: Vec::new(),
        };
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&dec3, &[]).expect("dec3"))
                .expect("ec-3 sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_536, true),
                test_prepared_sample(1_536, 1_536, true),
                test_prepared_sample(3_072, 1_536, true),
            ],
            chunk_sample_counts: vec![3],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 4_608,
            presentation_duration_media: 4_608,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 4_608);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_odd_640k_ec3_sample_count() {
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"ec-3"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 48_000 << 16,
            ..AudioSampleEntry::default()
        };
        let dec3 = Dec3 {
            data_rate: 640,
            num_ind_sub: 0,
            ec3_substreams: vec![crate::boxes::etsi_ts_102_366::Ec3Substream::default()],
            reserved: Vec::new(),
        };
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&dec3, &[]).expect("dec3"))
                .expect("ec-3 sample entry");
        let config = MuxTrackConfig::new_audio(1, 48_000, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample(0, 1_536, true),
                test_prepared_sample(1_536, 1_536, true),
                test_prepared_sample(3_072, 1_536, true),
            ],
            chunk_sample_counts: vec![3],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 4_608,
            presentation_duration_media: 4_608,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(48_000, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 4_608);
    }

    #[test]
    fn fragmented_mehd_duration_preserves_even_full_frame_192k_mp4a() {
        let mut esds = crate::boxes::iso14496_14::Esds::default();
        esds.descriptors = vec![
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication: 0x40,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: 2,
                data: vec![0x12, 0x10],
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
        ];
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 44_100 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");
        let config = MuxTrackConfig::new_audio(1, 44_100, sample_entry_box.clone());
        let track = PreparedTrack {
            config: &config,
            sample_entry_box: &sample_entry_box,
            samples: vec![
                test_prepared_sample_with_size(0, 1_024, 548, true),
                test_prepared_sample_with_size(1_024, 1_024, 548, true),
            ],
            chunk_sample_counts: vec![2],
            fragmented_reference_group_fragment_counts: None,
            media_duration: 2_048,
            presentation_duration_media: 2_048,
            edit_media_time: None,
            flat_timing_override: None,
        };

        let duration = fragmented_mehd_duration(44_100, &track).expect("fragmented mehd duration");

        assert_eq!(duration, 2_048);
    }

    #[test]
    fn fragmented_mp4a_sample_entry_sample_rate_falls_back_to_sample_entry_header() {
        let mut esds = Esds::default();
        esds.descriptors = vec![
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_CONFIG_DESCRIPTOR_TAG,
                decoder_config_descriptor: Some(
                    crate::boxes::iso14496_14::DecoderConfigDescriptor {
                        object_type_indication: 0x40,
                        stream_type: 5,
                        reserved: true,
                        ..crate::boxes::iso14496_14::DecoderConfigDescriptor::default()
                    },
                ),
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
            crate::boxes::iso14496_14::Descriptor {
                tag: crate::boxes::iso14496_14::DECODER_SPECIFIC_INFO_TAG,
                size: 1,
                data: vec![0xff],
                ..crate::boxes::iso14496_14::Descriptor::default()
            },
        ];
        let sample_entry = AudioSampleEntry {
            sample_entry: SampleEntry {
                box_type: FourCc::from_bytes(*b"mp4a"),
                data_reference_index: 1,
            },
            channel_count: 2,
            sample_size: 16,
            sample_rate: 44_100 << 16,
            ..AudioSampleEntry::default()
        };
        let sample_entry_box =
            encode_typed_box(&sample_entry, &encode_typed_box(&esds, &[]).expect("esds"))
                .expect("mp4a sample entry");

        let sample_rate =
            fragmented_mp4a_sample_entry_sample_rate(&sample_entry_box).expect("sample rate");

        assert_eq!(sample_rate, 44_100 << 16);
    }
}
