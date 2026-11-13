{
  description = "Chatting handler and worker packages";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      version = (builtins.fromTOML (builtins.readFile ./pyproject.toml)).project.version;
      revision = self.shortRev or self.dirtyShortRev or "unknown";
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        rec {
          chatting-handler = pkgs.callPackage ./nix/handler.nix {
            src = self;
            inherit version revision;
          };
          chatting-worker = pkgs.callPackage ./nix/worker.nix {
            src = self;
            inherit version;
          };
          chatting = pkgs.symlinkJoin {
            name = "chatting-${version}";
            paths = [
              chatting-handler
              chatting-worker
            ];
          };
          default = chatting;
        }
      );

      checks = forAllSystems (system: {
        inherit (self.packages.${system}) chatting;
      });
      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixfmt);
    };
}
