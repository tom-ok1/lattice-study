fn main() {
    let proto = "../../proto/control.proto";
    println!("cargo:rerun-if-changed={proto}");

    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("the vendored protoc binary must be available");
    std::env::set_var("PROTOC", protoc);
    prost_build::Config::new()
        .compile_protos(&[proto], &["../../proto"])
        .expect("control protobuf definitions must compile");
}
