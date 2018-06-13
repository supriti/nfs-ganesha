/*
 * Copyright © 2012-2014, CohortFS, LLC.
 * Author: Adam C. Emerson <aemerson@linuxbox.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

#include "gsh_rpc.h"
#include <cephfs/libcephfs.h>
#include "fsal.h"
#include "fsal_types.h"
#include "fsal_api.h"
#include "fsal_up.h"
#include "pnfs_utils.h"
#include "internal.h"
#include "nfs_exports.h"
#include "FSAL/fsal_commonlib.h"
#include "statx_compat.h"

/**
 * Linux supports a stripe pattern with no more than 4096 stripes, but
 * for now we stick to 1024 to keep them da_addrs from being too
 * gigantic.
 */

static const size_t BIGGEST_PATTERN = 1024;

#if CEPH_PNFS
/**
 * @file   FSAL_CEPH/mds.c
 * @author Adam C. Emerson <aemerson@linuxbox.com>
 * @date Wed Oct 22 13:24:33 2014
 *
 * @brief pNFS Metadata Server Operations for the Ceph FSAL
 *
 * This file implements the layoutget, layoutreturn, layoutcommit,
 * getdeviceinfo, and getdevicelist operations and export query
 * support for the Ceph FSAL.
 */
static bool initiate_recall(vinodeno_t vi, bool write, void *opaque)
{
	/* The private 'full' object handle */
	struct ceph_handle *handle = (struct ceph_handle *)opaque;
	/* Return code from upcall operation */
	state_status_t status = STATE_SUCCESS;
	struct gsh_buffdesc key = {
		.addr = &handle->vi,
		.len = sizeof(vinodeno_t)
	};
	struct pnfs_segment segment = {
		.offset = 0,
		.length = UINT64_MAX,
		.io_mode = (write ? LAYOUTIOMODE4_RW : LAYOUTIOMODE4_READ)
	};

	status = handle->up_ops->layoutrecall(&key, LAYOUT4_FLEX_FILES,
		false, &segment, NULL, NULL);

	if (status != STATE_SUCCESS)
		return false;

	return true;
}
#endif

/*
* Once a device ID is
	 deleted by the server, the server MUST NOT reuse the device ID for
	 the same layout type and client ID again.??????
*/
/**
 * @brief Describe a Ceph striping pattern
 *
 * At present, we support flex files based layout
 * only. The CRUSH striping pattern is a-periodic.
 *
 * @param[in]  export_pub   Public export handle
 * @param[out] da_addr_body Stream we write the result to
 * @param[in]  type         Type of layout that gave the device
 * @param[in]  deviceid     The device to look up
 *
 * @return Valid error codes in RFC 5661, p. 365.
 */
nfsstat4 getdeviceinfo(struct fsal_module *fsal_hdl,
					XDR *da_addr_body,
					const layouttype4 type,
					const struct pnfs_deviceid *deviceid)
{
	struct ceph_export *export = NULL;
	struct fsal_export *export_hdl;
	struct ceph_file_layout file_layout;
	struct glist_head *glist, *glistn;
	struct Inode *inode = NULL;
	vinodeno_t vinode;
	unsigned int num_osds;
	size_t osd = 0;
	nfsstat4 nfs_status = 0;
	uint16_t export_id;


	/*
	 * SUPU: Adding these values here. Need to populate them properly
	*/
	const bool_t ffdv_tightly_coupled = 1;
	const uint32_t ffdv_wsize = 1048576;
	const uint32_t ffdv_rsize = 1048576;
	const uint32_t ffdv_minorversion = 1;
	const uint32_t ffdv_version = 4;
	const u_int multipath_list4_len = 1;
	const u_int ffda_versions_len = 1;

	if (type != LAYOUT4_FLEX_FILES) {
		LogCrit(COMPONENT_PNFS, "Unsupported layout type: %x", type);
		return NFS4ERR_UNKNOWN_LAYOUTTYPE;
	}

	/* Extract ceph_export */
	export_id = deviceid->device_id2;
	glist_for_each_safe(glist, glistn, &fsal_hdl->exports) {
		export_hdl = glist_entry(glist, struct fsal_export, exports);
		if (export_hdl->export_id == export_id) {
			export = container_of(export_hdl,
				struct ceph_export, export);
			break;
		}
	}

	if (!export) {
		LogCrit(COMPONENT_PNFS,
			"Couldn't find export with id: %" PRIu16, export_id);
		return NFS4ERR_SERVERFAULT;
	}

	vinode.ino.val = deviceid->devid;
	vinode.snapid.val = CEPH_NOSNAP;
	inode = ceph_ll_get_inode(export->cmount, vinode);

	num_osds = ceph_ll_num_osds(export->cmount);

	/* Retrieve and calculate storage parameters of layout */
	memset(&file_layout, 0, sizeof(struct ceph_file_layout));
	if (ceph_ll_file_layout(export->cmount, inode, &file_layout) != 0) {
		LogCrit(COMPONENT_PNFS, "Failed to get Ceph layout for inode");
		return NFS4ERR_SERVERFAULT;
	}

	if (type != LAYOUT4_FLEX_FILES) {
		LogMajor(COMPONENT_PNFS, "Unsupported layout type: %x", type);
		return NFS4ERR_UNKNOWN_LAYOUTTYPE;
	}

	/* Since our index is the OSD number itself, we have only one
		host per multipath_list. */
	num_osds = 1;
	for (osd = 0; osd < num_osds; osd++) {
		fsal_multipath_member_t host;

		memset(&host, 0, sizeof(fsal_multipath_member_t));
		host.proto = 6;
		if (ceph_ll_osdaddr(export->cmount, osd, &host.addr) < 0) {
			LogCrit(COMPONENT_PNFS,
				"Unable to get IP address for OSD %lu.", osd);
			return NFS4ERR_SERVERFAULT;
		}

		host.port = 2049;
		nfs_status =
			FSAL_encode_ff_device_versions4(da_addr_body,
				multipath_list4_len, ffda_versions_len,
				&host, ffdv_version,
				ffdv_minorversion, ffdv_rsize,
				ffdv_wsize, ffdv_tightly_coupled);

		if (nfs_status != NFS4_OK)
			return nfs_status;
	}
	return NFS4_OK;
}

/**
 * @brief Get list of available devices
 *
 * We do not support listing devices and just set EOF without doing
 * anything.
 *
 * @param[in]     export_pub Export handle
 * @param[in]     type      Type of layout to get devices for
 * @param[in]     cb        Function taking device ID halves
 * @param[in,out] res       In/out and output arguments of the function
 *
 * @return Valid error codes in RFC 5661, pp. 365-6.
 */

static nfsstat4 getdevicelist(struct fsal_export *export_pub,
					layouttype4 type, void *opaque,
					bool (*cb)(void *opaque,
							const uint64_t id),
					struct fsal_getdevicelist_res *res)
{
	res->eof = true;
	return NFS4_OK;
}

/**
 * @brief Get layout types supported by export
 *
 * We just return a pointer to the single type and set the count to 1.
 *
 * @param[in]  export_pub Public export handle
 * @param[out] count      Number of layout types in array
 * @param[out] types      Static array of layout types that must not be
 *                        freed or modified and must not be dereferenced
 *                        after export reference is relinquished
 */

static void fs_layouttypes(struct fsal_export *export_pub,
						int32_t *count,
						const layouttype4 **types)
{
	static const layouttype4 supported_layout_type = LAYOUT4_FLEX_FILES;
	*types = &supported_layout_type;
	*count = 1;
}

/**
 * @brief Get layout block size for export
 *
 * This function just returns the Ceph default.
 *
 * @param[in] export_pub Public export handle
 *
 * @return 4 MB.
 */

static uint32_t fs_layout_blocksize(struct fsal_export *export_pub)
{
	return 0x400000;
}

/**
 * @brief Maximum number of segments we will use
 *
 * Since current clients only support 1, that's what we'll use.
 *
 * @param[in] export_pub Public export handle
 *
 * @return 1
 */
static uint32_t fs_maximum_segments(struct fsal_export *export_pub)
{
	return 1;
}

/**
 * @brief Size of the buffer needed for a loc_body
 *
 * Just a handle plus a bit.
 *
 * @param[in] export_pub Public export handle
 *
 * @return Size of the buffer needed for a loc_body
 */
static size_t fs_loc_body_size(struct fsal_export *export_pub)
{
	return 0x100;
}

/**
 * @brief Size of the buffer needed for a ds_addr
 *
 * This one is huge, due to the striping pattern.
 *
 * @param[in] export_pub Public export handle
 *
 * @return Size of the buffer needed for a ds_addr
 */
size_t fs_da_addr_size(struct fsal_module *fsal_hdl)
{
	return 0x1400;
}

void fsal_ops_pnfs(struct fsal_ops *ops)
{
	ops->getdeviceinfo = getdeviceinfo;
	ops->fs_da_addr_size = fs_da_addr_size;
}

void export_ops_pnfs(struct export_ops *ops)
{
	ops->getdevicelist = getdevicelist;
	ops->fs_layouttypes = fs_layouttypes;
	ops->fs_layout_blocksize = fs_layout_blocksize;
	ops->fs_maximum_segments = fs_maximum_segments;
	ops->fs_loc_body_size = fs_loc_body_size;

}


/**
 * @brief Grant a layout segment.
 *
 * Grant a layout on a subset of a file requested.  As a special case,
 * lie and grant a whole-file layout if requested, because Linux will
 * ignore it otherwise.
 *
 * @param[in]     obj_pub  Public object handle
 * @param[in]     req_ctx  Request context
 * @param[out]    loc_body An XDR stream to which the FSAL must encode
 *                         the layout specific portion of the granted
 *                         layout segment.
 * @param[in]     arg      Input arguments of the function
 * @param[in,out] res      In/out and output arguments of the function
 *
 * @return Valid error codes in RFC 5661, pp. 366-7.
 */

static nfsstat4 layoutget(struct fsal_obj_handle *obj_pub,
				struct req_op_context *req_ctx, XDR *loc_body,
				const struct fsal_layoutget_arg *arg,
				struct fsal_layoutget_res *res)
{
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	struct ceph_handle *handle =
		container_of(obj_pub, struct ceph_handle, handle);
	struct ceph_file_layout file_layout;
	struct ceph_ds_wire ds_wire;
	struct pnfs_deviceid deviceid =
			DEVICE_ID_INIT_ZERO(FSAL_ID_CEPH);
	uint32_t stripe_width = 0;
	uint64_t last_possible_byte = 0;
	nfsstat4 nfs_status = 0;

	/* We support only LAYOUT4_FLEX_FILES layouts */
	if (arg->type != LAYOUT4_FLEX_FILES) {
		LogCrit(COMPONENT_PNFS, "Unsupported layout type: %x",
			arg->type);
		return NFS4ERR_UNKNOWN_LAYOUTTYPE;
	}

	struct gsh_buffdesc ds_desc = {
		.addr = &ds_wire,
		.len = sizeof(struct ceph_ds_wire)
	};

	struct pnfs_segment smallest_acceptable = {
		.io_mode = res->segment.io_mode,
		.offset = res->segment.offset,
		.length = arg->minlength
	};

	struct pnfs_segment forbidden_area = {
		.io_mode = res->segment.io_mode,
		.length = NFS4_UINT64_MAX
	};


	memset(&file_layout, 0, sizeof(struct ceph_file_layout));
	ceph_ll_file_layout(export->cmount, handle->i, &file_layout);

	stripe_width = file_layout.fl_stripe_unit;
	last_possible_byte = (BIGGEST_PATTERN * stripe_width) - 1;
	forbidden_area.offset = last_possible_byte + 1;

	/* Since the Linux kernel refuses to work with any layout that
		 doesn't cover the whole file, if a whole file layout is
		 requested, lie.

		 Otherwise, make sure the required layout doesn't go beyond
		 what can be accessed through pNFS. This is a preliminary
		 check before even talking to Ceph. */
	if (!((res->segment.offset == 0) &&
		 (res->segment.length == NFS4_UINT64_MAX))) {
		if (pnfs_segments_overlap(&smallest_acceptable,
			&forbidden_area)) {
			LogCrit(COMPONENT_PNFS,
				"Required layout extends beyond allowed region. offset: %"
				PRIu64 ", minlength: %" PRIu64 ".",
				res->segment.offset, arg->minlength);
			return NFS4ERR_BADLAYOUT;
		}
		res->segment.offset = 0;
		res->segment.length = stripe_width * BIGGEST_PATTERN;
	}

	LogDebug(COMPONENT_PNFS,
				"will issue layout offset: %"
				PRIu64 " length: %" PRIu64,
				res->segment.offset, res->segment.length);

	/* We are using sparse layouts with commit-through-DS, so our
		 utility word contains only the stripe width, our first
		 stripe is always at the beginning of the layout, and there
		 is no pattern offset. */
	if ((stripe_width & ~NFL4_UFLG_STRIPE_UNIT_SIZE_MASK) != 0) {
		LogCrit(COMPONENT_PNFS,
			"Ceph returned stripe width that is disallowed by NFS: %"
			PRIu32 ".", stripe_width);
		return NFS4ERR_SERVERFAULT;
	}

	/* For now, just make the low quad of the deviceid be the
	 * inode number.  With the span of the layouts constrained
	 * above, this lets us generate the device address on the fly
	 * from the deviceid rather than storing it.
	 */

	deviceid.device_id2 = arg->export_id;
	deviceid.devid = handle->vi.ino.val;

	ds_wire.vi = handle->vi;
	ds_wire.layout = file_layout;

	/*SUPU: here we need a way to encode ffds_user/ffds_group*/
	fattr4_owner ffds_user;

	ffds_user.utf8string_val = "19452";
	ffds_user.utf8string_len = strlen(ffds_user.utf8string_val);

	uint32_t ffds_efficieny = 1;
	nfs_status =
		FSAL_encode_flex_file_layout(loc_body,
					&deviceid, 0, 1, 1, 1,
					&req_ctx->fsal_export->export_id,
					&ds_desc, ffds_efficieny,
					ffds_user, ffds_user,
					FF_FLAGS_NO_IO_THRU_MDS, 0);

	if (nfs_status != NFS4_OK) {
		LogCrit(COMPONENT_PNFS,
			"Failed to encode nfsv4_1_file_layout.");
		goto relinquish;
	}

	/* We grant only one segment, and we want it back when the file
		 is closed. */
	res->return_on_close = true;
	res->last_segment = true;

relinquish:
	return nfs_status;

}

/**
 * @brief Potentially return one layout segment
 *
 * Since we don't make any reservations, in this version, or get any
 * pins to release, always succeed
 *
 * @param[in] obj_pub  Public object handle
 * @param[in] req_ctx  Request context
 * @param[in] lrf_body Nothing for us
 * @param[in] arg      Input arguments of the function
 *
 * @return Valid error codes in RFC 5661, p. 367.
 */

static nfsstat4 layoutreturn(struct fsal_obj_handle *obj_pub,
				struct req_op_context *req_ctx, XDR *lrf_body,
				const struct fsal_layoutreturn_arg *arg)
{
	/*
	 * SUPU: Here the stateid: seqid must be incremented by one.
	 * https://tools.ietf.org/html/rfc5661#section-12.5.2
	*/
#if 0
	/* The private 'full' export */
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	/* The private 'full' object handle */
	struct ceph_handle *handle =
		container_of(obj_pub, struct ceph_handle, handle);

	/* Sanity check on type */
	if (arg->lo_type != LAYOUT4_FLEX_FILES) {
		LogCrit(COMPONENT_PNFS, "Unsupported layout type: %x",
			arg->lo_type);
		return NFS4ERR_UNKNOWN_LAYOUTTYPE;
	}

	if (arg->dispose) {
		PTHREAD_RWLOCK_wrlock(&handle->handle.obj_lock);
		if (arg->cur_segment.io_mode
				== LAYOUTIOMODE4_READ) {
			if (--handle->rd_issued != 0) {
				PTHREAD_RWLOCK_unlock(&handle->handle.obj_lock);
				return NFS4_OK;
			}
		} else {
			if (--handle->rd_issued != 0) {
				PTHREAD_RWLOCK_unlock(&handle->handle.obj_lock);
				return NFS4_OK;
			}
		}

#if 0
	ceph_ll_return_rw(export->cmount, handle->wire.vi,
			arg->cur_segment.io_mode == LAYOUTIOMODE4_READ
			? handle->rd_serial
			: handle->rw_serial);
#endif

		PTHREAD_RWLOCK_unlock(&handle->handle.obj_lock);
	}
#endif
	/* SUPU: as there is no implementation of layoutreturn
	 * the pnfs client keeps looping. hence trying to send
	 * error to see how it behaves when it gets the error
	 */
	return NFS4ERR_SERVERFAULT;
}

/**
 * @brief Commit a segment of a layout
 *
 * Update the size and time for a file accessed through a layout.
 *
 * @param[in]     obj_pub  Public object handle
 * @param[in]     req_ctx  Request context
 * @param[in]     lou_body An XDR stream containing the layout
 *                         type-specific portion of the LAYOUTCOMMIT
 *                         arguments.
 * @param[in]     arg      Input arguments of the function
 * @param[in,out] res      In/out and output arguments of the function
 *
 * @return Valid error codes in RFC 5661, p. 366.
 */

static nfsstat4 layoutcommit(struct fsal_obj_handle *obj_pub,
				struct req_op_context *req_ctx, XDR *lou_body,
				const struct fsal_layoutcommit_arg *arg,
				struct fsal_layoutcommit_res *res)
{
#if 0
	/* The private 'full' export */
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	/* The private 'full' object handle */
	struct ceph_handle *handle =
		container_of(obj_pub, struct ceph_handle, handle);
	/* Old stat, so we don't truncate file or reverse time */
	struct ceph_statx stxold;
	/* new stat to set time and size */
	struct ceph_statx stxnew;
	/* Mask to determine exactly what gets set */
	int attrmask = 0;
	/* Error returns from Ceph */
	int ceph_status = 0;

	/* Sanity check on type */
	if (arg->type != LAYOUT4_FLEX_FILES) {
		LogCrit(COMPONENT_PNFS, "Unsupported layout type: %x",
			arg->type);
		return NFS4ERR_UNKNOWN_LAYOUTTYPE;
	}

	/* A more proper and robust implementation of this would use
		 Ceph caps, but we need to hack at the client to expose
		 those before it can work. */
	ceph_status = fsal_ceph_ll_getattr(export->cmount, handle->i,
			&stxold, CEPH_STATX_SIZE|CEPH_STATX_MTIME,
			op_ctx->creds);
	if (ceph_status < 0) {
		LogCrit(COMPONENT_PNFS,
			"Error %d in attempt to get attributes of file %"
			PRIu64 ".", -ceph_status, handle->vi); */
		return posix2nfs4_error(-ceph_status);
	}

	stxnew.stx_mask = 0;
	if (arg->new_offset) {
		if (stxold.stx_size < arg->last_write + 1) {
			attrmask |= CEPH_SETATTR_SIZE;
			stxnew.stx_size = arg->last_write + 1;
			res->size_supplied = true;
			res->new_size = arg->last_write + 1;
		}
	}

	if (arg->time_changed &&
			(arg->new_time.seconds > stxold.stx_mtime ||
			(arg->new_time.seconds == stxold.stx_mtime &&
			arg->new_time.nseconds > stxold.stx_mtime_ns))) {
		stxnew.stx_mtime = arg->new_time.seconds;
		stxnew.stx_mtime_ns = arg->new_time.nseconds;
	} else {
		struct timespec now;

		ceph_status = clock_gettime(CLOCK_REALTIME, &now);
		if (ceph_status != 0)
			return posix2nfs4_error(errno);
		stxnew.stx_mtime = now.tv_sec;
		stxnew.stx_mtime_ns = now.tv_nsec;
	}

	attrmask |= CEPH_SETATTR_MTIME;

	ceph_status = fsal_ceph_ll_setattr(export->cmount, handle->wire.vi,
					&stxnew, attrmask, op_ctx->creds);
	if (ceph_status < 0) {
		LogCrit(COMPONENT_PNFS,
			"Error %d in attempt to get attributes of file %"
			PRIu64 ".", -ceph_status, handle->wire.vi.ino.val);
		return posix2nfs4_error(-ceph_status);
	}

	/* This is likely universal for files. */
	res->commit_done = true;
#endif
	return NFS4_OK;
}

void handle_ops_pnfs(struct fsal_obj_ops *ops)
{
	ops->layoutget = layoutget;
	ops->layoutreturn = layoutreturn;
	ops->layoutcommit = layoutcommit;
}

