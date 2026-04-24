// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::write_session_coordinator::WriteSessionCoordinator;
use super::FsCore;
use crate::service::domain::{
    CloseWriteInput, CloseWriteOutput, CoreResult, FsyncBarrierInput, FsyncBarrierOutput, OpenWriteInput,
    OpenWriteOutput, ReleaseSessionInput, ReleaseSessionOutput, RenewLeaseInput, RenewLeaseOutput, RequestContext,
    SessionGuardInputs,
};

impl FsCore {
    pub(crate) async fn plan_session(
        &self,
        req_ctx: &RequestContext,
        file_handle: u64,
    ) -> CoreResult<SessionGuardInputs> {
        let session = self.write_session_manager.get_session(file_handle);
        self.success(
            req_ctx,
            SessionGuardInputs {
                file_handle,
                inode_id: session.as_ref().map(|s| s.inode_id),
                mount_id: session.as_ref().map(|s| s.mount_id),
            },
            None,
            None,
        )
    }

    pub(crate) fn write_session_for_handle(&self, file_handle: u64) -> Option<crate::write_session::WriteSession> {
        self.write_session_manager.get_session(file_handle)
    }

    pub(crate) async fn execute_release(&self, req: ReleaseSessionInput) -> CoreResult<ReleaseSessionOutput> {
        WriteSessionCoordinator::new(self).execute_release(req).await
    }

    pub(crate) async fn execute_renew_inode_lease(&self, req: RenewLeaseInput) -> CoreResult<RenewLeaseOutput> {
        WriteSessionCoordinator::new(self).execute_renew_inode_lease(req).await
    }

    pub(crate) async fn execute_open_write(&self, req: OpenWriteInput) -> CoreResult<OpenWriteOutput> {
        WriteSessionCoordinator::new(self).execute_open_write(req).await
    }

    pub(crate) async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        WriteSessionCoordinator::new(self).execute_close_write(req).await
    }

    pub(crate) async fn execute_fsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        WriteSessionCoordinator::new(self).execute_fsync(req).await
    }

    pub(crate) async fn execute_hsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }

    pub(crate) async fn execute_hflush(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }
}
