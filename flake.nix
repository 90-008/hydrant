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
        let
          redirects = pkgs.writeTextFile {
            name = "_redirects";
            text = ''
            /static/* /static/:splat 200!
            /docs/w/* /:splat 301!
            /~ / 301!
            / /docs/w/~ 200!
            /* /docs/w/:splat 200!
            '';
          };
          buildDocs = pkgs.writeShellApplication {
            name = "build-docs";
            runtimeInputs = [ inputs'.verbiage.packages.build ];
            text = ''
              VERBIAGE_DATA="$(realpath "''${1:-$(pwd)/docs}")"
              export VERBIAGE_DATA
              VERBIAGE_TITLE="hydrant"
              export VERBIAGE_TITLE
              out="''${2:-$(pwd)/docs-dist}"
              verbiage-build docs "$out"
              cp -f ${redirects} "$out/_redirects"
            '';
          };
          deployDocs = pkgs.writeShellApplication {
            name = "deploy-docs";
            runtimeInputs = [ buildDocs pkgs.bun ];
            text = ''
              out="$(pwd)/docs-dist"
              build-docs "$(pwd)/docs" "$out"
              bunx wispctl -y --path "$out" --site hydrant-docs did:plc:dfl62fgb7wtjj3fcbb72naae
            '';
          };
        in
        {
          nci.projects."hydrant" = {
            path = ./.;
            export = false;
          };
          packages.default = pkgs.callPackage ./default.nix {};
          apps.build-docs = { type = "app"; program = "${buildDocs}/bin/build-docs"; };
          apps.deploy-docs = { type = "app"; program = "${deployDocs}/bin/deploy-docs"; };
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
              (pkgs.writeShellApplication {
                name = "verbiage";
                runtimeInputs = [ inputs'.verbiage.packages.default ];
                text = ''
                  VERBIAGE_TITLE="hydrant" verbiage "$@"
                '';
              })
            ]);
          });
        };
    };
}
