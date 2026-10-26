use super::*;

/// A real directory controller job and gateway join never read sibling session authority.
#[tokio::test(flavor = "multi_thread")]
async fn directory_workspace_jobs_ignore_corrupt_and_missing_sibling_grants() {
    use cowshed_core::storage::StorageLayout;
    use cowshed_types::metadata::{DetachedWorkspaceMetadata, sidecar_path};
    let mut fixture = Fixture::directory();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    fixture.start_gateway().await;
    service.reconcile_gateway().await.unwrap();
    succeed(&mut service, ["fork", "selected"]).await;
    succeed(&mut service, ["fork", "sibling"]).await;
    service.reconcile_gateway().await.unwrap();
    let gateway = fixture.gateway.as_ref().unwrap().handle();
    let installed = gateway.status().await.unwrap();
    let repo = RepoId::parse("fixture/dispatch").unwrap();
    let storage = StorageLayout::new(fixture.storage.store(), &repo).unwrap();
    let sibling = storage
        .session_image(&WorkspaceName::session("sibling").unwrap())
        .unwrap()
        .image()
        .to_owned();
    let sidecar = sidecar_path(&sibling);
    let original = fs::read(&sidecar).unwrap();
    let metadata = DetachedWorkspaceMetadata::read_for_image(&sibling).unwrap();
    for corrupt in [true, false] {
        if corrupt {
            fs::write(&sidecar, b"{ corrupt").unwrap();
        } else {
            fs::remove_file(&sidecar).unwrap();
        }
        service
            .shutdown()
            .await
            .expect("stop the controller before a cold named open");
        service = ActorBridge::open_directory_existing_at(
            &fixture.checkout,
            RecoveryScope::Workspaces([WorkspaceName::new("selected").unwrap()].into()),
            fixture.storage.clone(),
        )
        .await
        .expect("cold selected controller does not read sibling grants");
        let (stdout, _) = succeed(
            &mut service,
            [
                "exec",
                "selected",
                "--",
                "/bin/sh",
                "-c",
                "printf 'isolated\\n'",
            ],
        )
        .await;
        assert_eq!(stdout, b"isolated\n");
        let (stdout, _) = succeed(
            &mut service,
            [
                "exec",
                "selected",
                "--class",
                "gate",
                "--",
                "/bin/sh",
                "-c",
                "printf 'trusted\n'",
            ],
        )
        .await;
        assert_eq!(
            stdout,
            b"trusted
"
        );
        let current = gateway.status().await.unwrap();
        assert_eq!(
            current
                .sessions
                .iter()
                .map(|session| (&session.workspace_id, session.revision, &session.endpoint))
                .collect::<std::collections::BTreeSet<_>>(),
            installed
                .sessions
                .iter()
                .map(|session| (&session.workspace_id, session.revision, &session.endpoint))
                .collect::<std::collections::BTreeSet<_>>(),
            "a job must not evict or retarget sibling sessions"
        );
        if corrupt {
            assert!(
                service.list(None).await.is_err(),
                "project diagnostics still read the corrupt sibling"
            );
        }
        metadata.write_for_image(&sibling).unwrap();
    }
    assert_eq!(fs::read(&sidecar).unwrap(), original);
    service.shutdown().await.unwrap();
    fixture.stop_gateway().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn directory_egress_revoke_does_not_reconcile_an_absent_gateway() {
    let fixture = Fixture::directory();
    let mut service = fixture.open().await;
    adopt(&fixture, &mut service).await;
    let control = fixture.storage.control_socket();
    assert!(
        !control
            .try_exists()
            .expect("observe fixture control socket")
    );

    for (project_wide, all) in [(false, false), (true, false), (false, true), (true, true)] {
        let target = if project_wide {
            "--project-wide"
        } else {
            "main"
        };
        succeed(
            &mut service,
            ["grant", target, "--egress", "api.example.test"],
        )
        .await;
        let command = if all {
            vec!["revoke", target, "--all"]
        } else {
            vec!["revoke", target, "--egress", "api.example.test"]
        };
        succeed(&mut service, command).await;
        service
            .shutdown()
            .await
            .expect("stop the writer before checking durable grants");
        service = fixture.reopen().await;
        if project_wide {
            let grants = service
                .project_grants()
                .await
                .expect("read durable project grants");
            assert!(
                grants
                    .egress
                    .iter()
                    .all(|rule| rule.host != "api.example.test")
            );
        } else {
            let grants = service
                .grants("main")
                .await
                .expect("read durable workspace grants");
            assert!(
                grants
                    .egress
                    .iter()
                    .all(|rule| rule.host != "api.example.test")
            );
        }
        assert!(
            !control
                .try_exists()
                .expect("no gateway was started by recovery")
        );
    }
    service
        .shutdown()
        .await
        .expect("stop revoke recovery controller");
}
