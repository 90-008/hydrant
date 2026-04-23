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
      "fjall-3.1.4" = "sha256-QO1EvdSwAetH24TOxiHSkguE4JBKViJy4JXQvi9hi/A=";
      "lsm-tree-3.1.4" = "sha256-6fhVvfuZQXeUIXmDAaHpEDLZwZsMTLCMNQAAac3/WkM=";
    };
  };
}
