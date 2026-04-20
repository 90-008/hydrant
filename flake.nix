{
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
  inputs.nci.url = "github:90-008/nix-cargo-integration";
  inputs.verbiage.url = "github:90-008/verbiage";

  outputs =
    inp:
    inp.parts.lib.mkFlake { inputs = inp; } {
      systems = [ "x86_64-linux" ];
      imports = [inp.nci.flakeModule];
      perSystem =
        {
          pkgs,
          config,
          inputs',
          ...
        }:
        {
          nci.projects."hydrant" = {
            path = ./.;
            export = false;
          };
          packages.default = pkgs.callPackage ./default.nix {};
          devShells.default = config.nci.outputs."hydrant".devShell.overrideAttrs (old: {
            packages = (old.packages or []) ++ (with pkgs; [
              rustPlatform.rustLibSrc
              rust-analyzer
              cargo
              cargo-outdated
              rustc
              rustfmt
              gemini-cli
              go
              cmake
              websocat
              http-nu
              clang
              wild
              psmisc
              inputs'.verbiage.packages.default
            ]);
          });
        };
    };
}
