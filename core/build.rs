fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=version.txt");
    let version = std::fs::read_to_string("version.txt")?;
    println!("cargo:rustc-env=CORE_VERSION={}", version.trim());
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/telemetry_v1.proto"], &["proto"])?;
    Ok(())
}
