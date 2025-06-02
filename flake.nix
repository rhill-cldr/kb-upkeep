{
  description = "AI Chat Application";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
    devenv.url = "github:cachix/devenv";
  };
  nixConfig = {
    extra-trusted-public-keys = "devenv.cachix.org-1:w1cLUi8dv3hnoSPGAuibQv+f9TZLr6cv/Hm9XgU50cw=";
    extra-substituters = "https://devenv.cachix.org";
  };

  outputs = {
    self,
    nixpkgs,
    devenv,
    systems,
    ...
  } @ inputs: let
    forEachSystem = nixpkgs.lib.genAttrs (import systems);
  in {
    packages = forEachSystem (system: {
      devenv-up = self.devShells.${system}.default.config.procfileScript;
    });

    devShells = forEachSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      default = devenv.lib.mkShell {
        inherit inputs pkgs;
        modules = [
          {
            packages = with pkgs; [
              just
              grpc
              protobuf
              ruff
              spark
              zlib
            ];

            env = {
              PROTO_SRC = "src/main/proto";
              PYTHON_SRC = "src/main/python";
            };

            processes = {
              server.exec = "uv run src/main/python/server";
              inspector.exec = "npx @modelcontextprotocol/inspector";
            };

            services.postgres = {
              enable = true;
              package = pkgs.postgresql_14;
              initdbArgs = [
                "--locale=C"
                "--encoding=UTF8"
              ];
              initialDatabases = [
                {
                  name = "upkeep";
                }
              ];
              port = 5435;
            }; 
              
            tasks."upkeep:protoc" = {
                exec = ''protoc --proto_path=$PROTO_SRC --python_out=$PYTHON_SRC/server/api/ $PROTO_SRC/upkeep.proto'';
            };

            languages.javascript = {
              enable = true;
              npm.enable = true;
              npm.package = pkgs.nodejs_22;
              pnpm.enable = true;
              directory = "ui";
            };

            languages.python = {
              package = pkgs.python312;
              enable = true;
              uv = {
                enable = true;
                sync.enable = true;
                sync.allExtras = true;
              };
              venv = {enable = true;};
            };

            git-hooks.hooks = {
              ruff.enable = false;
              #shellcheck.enable = true;
              #markdownlint.enable = true;
              #alejandra.enable = true;
              #editorconfig-checker.enable = true;
            };

            dotenv.enable = true;

            env.LD_LIBRARY_PATH = with pkgs;
              lib.makeLibraryPath [
                zlib
              ];
          }
        ];
      };
    });
  };
}

