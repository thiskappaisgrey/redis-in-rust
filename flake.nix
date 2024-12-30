{
  description = "A basic rust flake with some tools";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      overlays = [
        (import rust-overlay)
        (final: prev: {
          cargo-valgrind = prev.cargo-valgrind.overrideAttrs rec {
            version = "main";
            src = prev.fetchFromGitHub {
              owner = "jfrimmel";
              repo = "cargo-valgrind";
              rev = "710756bb458bebc3637816d13df051ef2a10d5fe";
              sha256 = "sha256-uZmBMDvizwr2r4Ed4JqphmGsz4DJZ2vCGAA/I9kC3OA=";
            };
            cargoDeps = prev.rustPlatform.fetchCargoTarball ({
              inherit src;
              hash = "sha256-4/g/FTuUFX5wEqGl0Y21fJrOL49uapE5fUbJsEhaR4Y=";
            });
            doCheck = false;
          };
        })
      ];
      forAllSystems = function:
        nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-darwin" "x86_64-darwin" ]
        (system: function (import nixpkgs { inherit system overlays; }));
    in {
      devShells = forAllSystems (pkgs:
        let
          toolchain =
            pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in {
          default = pkgs.mkShell {
            nativeBuildInputs = [
              pkgs.pkg-config
              pkgs.clang
              # Mold Linker for faster builds (only on Linux)
              (pkgs.lib.optionals pkgs.stdenv.isLinux pkgs.mold)
            ];
            buildInputs = with pkgs; [
              openssl

              # this is for doing integration tests .. i.e. for scripting.
              # Not sure what language I prefer yet .. but probably julia > janet (I'm not a real lisper)
              janet
              julia
              valgrind
              just

              toolchain
              cargo-valgrind
            ];

            # This is needed or LSP won't work corretly
            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
            LD_LIBRARY_PATH =
              pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.gmp ];
          };
        });
    };
}
