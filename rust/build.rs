fn main() {
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src/pb")
        .compile(
            &[
                "proto/apache/rocketmq/v2/service.proto",
                "proto/apache/rocketmq/v2/admin.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
