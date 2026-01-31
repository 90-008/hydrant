{
  inputs.parts.url = "github:hercules-ci/flake-parts";
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

  outputs =
    inp:
    inp.parts.lib.mkFlake { inputs = inp; } {
      systems = [ "x86_64-linux" ];
      perSystem =
        {
          pkgs,
          config,
          ...
        }:
        {
          packages.default = pkgs.callPackage ./default.nix {};
          devShells = {
            default = pkgs.mkShell {
              packages = with pkgs; [
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
              ];
            };
          };
        };
    };
}
