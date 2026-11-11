{
  lib,
  buildGoModule,
  src ? ../.,
  version ? (builtins.fromTOML (builtins.readFile ../pyproject.toml)).project.version,
}:
buildGoModule {
  pname = "chatting-handler";
  inherit src version;
  modRoot = "go/handler";
  vendorHash = "sha256-JSdSizwhJKUMTLw8NqgTilkEWHf27gJWSweBd6DtJuE=";
  subPackages = [ "cmd/chatting-handler" ];
  checkPhase = ''
    runHook preCheck
    # Contract fixtures locate the repository via runtime.Caller. Nix's normal
    # trimmed build paths are correct for binaries, but not for these tests.
    go test -trimpath=false ./...
    runHook postCheck
  '';
  meta = {
    description = "Chatting task ingress, routing and message delivery handler";
    mainProgram = "chatting-handler";
    platforms = lib.platforms.linux;
  };
}
