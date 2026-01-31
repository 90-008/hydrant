{
  lib,
  rustPlatform,
  cmake,
  ...
}:
rustPlatform.buildRustPackage {
  pname = "hydrant";
  version = "main";

  src = lib.fileset.toSource {
    root = ./.;
    fileset = lib.fileset.unions [
      ./src ./Cargo.toml ./Cargo.lock
    ];
  };

  nativeBuildInputs = [cmake];

  cargoLock = {
    lockFile = ./Cargo.lock;
    outputHashes = {
      "jacquard-0.9.5" = "sha256-3bVpFW/qtP9cjkU2LflHjSI3mzw1iUPH+92n0zcOqDg=";
    };
  };
}
