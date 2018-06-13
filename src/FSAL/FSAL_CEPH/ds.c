/*
 * Copyright © 2012 CohortFS, LLC.
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

/**
 * @file   ds.c
 * @author Adam C. Emerson <aemerson@linuxbox.com>
 * @date   Mon Jul 30 12:29:22 2012
 *
 * @brief pNFS DS operations for Ceph
 *
 * This file implements the read, write, commit, and dispose
 * operations for Ceph data-server handles.
 *
 * Also, creating a data server handle -- now called via the DS itself.
 */



#include "config.h"

#include <cephfs/libcephfs.h>
#include <fcntl.h>
#include "fsal_api.h"
#include "FSAL/fsal_commonlib.h"
#include "../fsal_private.h"
#include "fsal_up.h"
#include "internal.h"
#include "pnfs_utils.h"

#define min(a, b) ({				\
	typeof(a) _a = (a);			\
	typeof(b) _b = (b);			\
	_a < _b ? _a : _b; })

/**
 * @brief Release a DS handle
 *
 * @param[in] ds_pub The object to release
 */
static void ds_release(struct fsal_ds_handle *const ds_pub)
{
	/* The private 'full' DS handle */
	struct ceph_ds_handle *ds =
		container_of(ds_pub, struct ceph_ds_handle, ds);

	fsal_ds_handle_fini(&ds->ds);
	gsh_free(ds);
}

/**
 * @brief Read from a data-server handle.
 *
 * NFSv4.1 data server handles are disjount from normal
 * filehandles (in Ganesha, there is a ds_flag in the filehandle_v4_t
 * structure) and do not get loaded into cache_inode or processed the
 * normal way.
 *
 * @param[in]  ds_pub           FSAL DS handle
 * @param[in]  req_ctx          Credentials
 * @param[in]  stateid          The stateid supplied with the READ operation,
 *                              for validation
 * @param[in]  offset           The offset at which to read
 * @param[in]  requested_length Length of read requested (and size of buffer)
 * @param[out] buffer           The buffer to which to store read data
 * @param[out] supplied_length  Length of data read
 * @param[out] eof              True on end of file
 *
 * @return An NFSv4.1 status code.
 */
static nfsstat4 ds_read(struct fsal_ds_handle *const ds_pub,
			struct req_op_context *const req_ctx,
			const stateid4 *stateid, const offset4 offset,
			const count4 requested_length, void *const buffer,
			count4 * const supplied_length,
			bool * const end_of_file)
{
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	struct ceph_ds_handle *ds =
		container_of(ds_pub, struct ceph_ds_handle, ds);
	int local_OSD = 0;
	uint32_t stripe_width = 0;
	uint64_t block_start = 0;
	uint32_t stripe = 0;
	uint32_t internal_offset = 0;
	int amount_read = 0;
	struct Inode *inode = NULL;

	local_OSD = ceph_get_local_osd(export->cmount);
	if (local_OSD < 0)
		return posix2nfs4_error(-local_OSD);

	stripe_width = ds->wire.layout.fl_stripe_unit;
	stripe = offset / stripe_width;
	block_start = stripe * stripe_width;
	internal_offset = offset - block_start;

	/* Get inode from vinode */
	inode = ceph_ll_get_inode(export->cmount, ds->wire.vi);

	/*if (local_OSD !=
			ceph_ll_get_stripe_osd(export->cmount, inode, stripe,
				   &(ds->wire.layout))) {
		return NFS4ERR_PNFS_IO_HOLE;
	}*/

	amount_read =
	ceph_ll_read_block(export->cmount, inode, stripe, buffer,
			       internal_offset,
			       min((stripe_width - internal_offset),
				   requested_length), &(ds->wire.layout));
	if (amount_read < 0)
		return posix2nfs4_error(-amount_read);

	*supplied_length = amount_read;

	*end_of_file = false;

	return NFS4_OK;
}

/**
 *
 * @brief Write to a data-server handle.
 *
 * This performs a DS write not going through the data server unless
 * FILE_SYNC4 is specified, in which case it connects the filehandle
 * and performs an MDS write.
 *
 * In case ffdv_tighly_coupuled is false, the writes MUST be committed
 * by the client to stable storage via issuing WRITEs with
 * stable_how == FILE_SYNC or by issuing a COMMIT after WRITEs
 * with stable_how != FILE_SYNC
 *
 * @param[in]  ds_pub           FSAL DS handle
 * @param[in]  req_ctx          Credentials
 * @param[in]  stateid          The stateid supplied with the READ operation,
 *                              for validation
 * @param[in]  offset           The offset at which to read
 * @param[in]  write_length     Length of write requested (and size of buffer)
 * @param[out] buffer           The buffer to which to store read data
 * @param[in]  stability wanted Stability of write
 * @param[out] written_length   Length of data written
 * @param[out] writeverf        Write verifier
 * @param[out] stability_got    Stability used for write (must be as
 *                              or more stable than request)
 *
 * @return An NFSv4.1 status code.
 */
static nfsstat4 ds_write(struct fsal_ds_handle *const ds_pub,
			 struct req_op_context *const req_ctx,
			 const stateid4 *stateid, const offset4 offset,
			 const count4 write_length, const void *buffer,
			 const stable_how4 stability_wanted,
			 count4 * const written_length,
			 verifier4 * const writeverf,
			 stable_how4 * const stability_got)
{
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	struct ceph_ds_handle *ds =
		container_of(ds_pub, struct ceph_ds_handle, ds);
	int local_OSD = 0;
	uint32_t stripe_width = 0;
	uint64_t block_start = 0;
	uint32_t stripe = 0;
	uint32_t internal_offset = 0;
	int32_t amount_written = 0;
	/* The adjusted write length, confined to one object */
	uint32_t adjusted_write = 0;
	struct Inode *inode = NULL;

	memset(*writeverf, 0, NFS4_VERIFIER_SIZE);

	/* Find out what my OSD ID is, so we can avoid talking to
	   other OSDs. */
	local_OSD = ceph_get_local_osd(export->cmount);
	if (local_OSD < 0)
		return posix2nfs4_error(-local_OSD);

	/* Find out what stripe we're writing to and where within the
	   stripe. */
	stripe_width = ds->wire.layout.fl_stripe_unit;
	stripe = offset / stripe_width;
	block_start = stripe * stripe_width;
	internal_offset = offset - block_start;

	/* Get inode from vinode */
	inode = ceph_ll_get_inode(export->cmount, ds->wire.vi);

#if 0
	/* Segmentation fault in this call disabling it for now */
	if (local_OSD !=
			ceph_ll_get_stripe_osd(export->cmount, inode, stripe,
				   &(ds->wire.layout))) {
		return NFS4ERR_PNFS_IO_HOLE;
	}
#endif

	adjusted_write = min((stripe_width - internal_offset), write_length);
	amount_written =
			ceph_ll_write_block(export->cmount, inode,
					stripe, (char *)buffer, internal_offset,
					adjusted_write, &(ds->wire.layout),
					ds->wire.snapseq,
				(stability_wanted == DATA_SYNC4));
	if (amount_written < 0)
		return posix2nfs4_error(-amount_written);

	*written_length = amount_written;
	*stability_got = stability_wanted;

	return NFS4_OK;
}

/**
 * @brief Commit a byte range to a DS handle.
 *
 * NFSv4.1 data server filehandles are disjount from normal
 * filehandles (in Ganesha, there is a ds_flag in the filehandle_v4_t
 * structure) and do not get loaded into cache_inode or processed the
 * normal way.
 *
 * @param[in]  ds_pub    FSAL DS handle
 * @param[in]  req_ctx   Credentials
 * @param[in]  offset    Start of commit window
 * @param[in]  count     Length of commit window
 * @param[out] writeverf Write verifier
 *
 * @return An NFSv4.1 status code.
 */
static nfsstat4 ds_commit(struct fsal_ds_handle *const ds_pub,
			  struct req_op_context *const req_ctx,
			  const offset4 offset, const count4 count,
			  verifier4 * const writeverf)
{
	struct ceph_export *export =
		container_of(req_ctx->fsal_export, struct ceph_export, export);
	struct ceph_ds_handle *ds =
		container_of(ds_pub, struct ceph_ds_handle, ds);
	struct Inode *inode = NULL;
	int rc = 0;

	/* Get inode from vinode */
	inode = ceph_ll_get_inode(export->cmount, ds->wire.vi);
	/* Find out what stripe we're writing to and where within the
	   stripe. */
	rc = ceph_ll_commit_blocks(export->cmount, inode, offset,
				   (count == 0) ? UINT64_MAX : count);
	if (rc < 0)
		return posix2nfs4_error(rc);
	memset(*writeverf, 0, NFS4_VERIFIER_SIZE);

	return NFS4_OK;
}

void dsh_ops_init(struct fsal_dsh_ops *ops)
{
	memcpy(ops, &def_dsh_ops, sizeof(struct fsal_dsh_ops));

	ops->release = ds_release;
	ops->read = ds_read;
	ops->write = ds_write;
	ops->commit = ds_commit;
}

/**
 * @brief Try to create a FSAL data server handle from a wire handle
 *
 * This function creates a FSAL data server handle from a client
 * supplied "wire" handle.  This is also where validation gets done,
 * since PUTFH is the only operation that can return
 * NFS4ERR_BADHANDLE.
 *
 * @param[in]  pds      FSAL pNFS DS
 * @param[in]  desc     Buffer from which to create the file
 * @param[out] handle   FSAL DS handle
 *
 * @return NFSv4.1 error codes.
 */
static nfsstat4 make_ds_handle(struct fsal_pnfs_ds *const pds,
			       const struct gsh_buffdesc *const desc,
			       struct fsal_ds_handle **const handle,
			       int flags)
{
	struct ceph_ds_wire *dsw =
		(struct ceph_ds_wire *)desc->addr;
	struct ceph_ds_handle *ds;

	*handle = NULL;

	if (desc->len != sizeof(struct ceph_ds_wire))
		return NFS4ERR_BADHANDLE;

	if (dsw->layout.fl_stripe_unit == 0)
		return NFS4ERR_BADHANDLE;

	ds = gsh_calloc(1, sizeof(struct ceph_ds_handle));

	*handle = &ds->ds;
	fsal_ds_handle_init(*handle, pds);
	memcpy(&ds->wire, desc->addr, desc->len);
	return NFS4_OK;
}

void pnfs_ds_ops_init(struct fsal_pnfs_ds_ops *ops)
{
	memcpy(ops, &def_pnfs_ds_ops, sizeof(struct fsal_pnfs_ds_ops));
	ops->make_ds_handle = make_ds_handle;
	ops->fsal_dsh_ops = dsh_ops_init;
}


