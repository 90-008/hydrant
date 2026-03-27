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
      "rmp-0.8.15" = "sha256-0VATbSR2lGiCJ8Ww4a5pkOHSRUjoysnFonpKS/oMzgU=";
      "fjall-3.1.2" = "sha256-6G2B9DDYCb8OLgT05uG1S2kvooDGKyydLr0tQfxkib0=";
      "lsm-tree-3.1.2" = "sha256-9tOdxX/3nZQkvF7dJSBXiTKZHcQPwCLFB+dwaD1j0ts=";
    };
  };
}
