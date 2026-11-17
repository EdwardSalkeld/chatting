{
  lib,
  buildNpmPackage,
  handler,
  src ? ../.,
}:
let
  frontend = buildNpmPackage {
    pname = "chatting-platform-ui";
    inherit (handler) version;
    src = lib.cleanSourceWith {
      src = src + "/platform";
      filter =
        path: type:
        lib.cleanSourceFilter path type
        && baseNameOf path != "node_modules"
        && !(lib.hasSuffix ".tsbuildinfo" path);
    };
    npmDepsHash = "sha256-/MNhGoFDU4CIt3GJCMw60rqXdsAt1QF7lM4Tge8tMcQ=";
    npmBuildFlags = [
      "--"
      "--outDir"
      "dist"
    ];
    doCheck = true;
    checkPhase = ''
      npm run format:check
      npm run test
    '';
    installPhase = ''
      mkdir -p "$out"
      cp -r dist/. "$out/"
    '';
  };
in
handler.overrideAttrs (old: {
  pname = "chatting-platform";
  inherit src;
  subPackages = [ "cmd/chatting-platform" ];
  preBuild = (old.preBuild or "") + ''
    mkdir -p internal/platform/web/dist
    cp -r ${frontend}/. internal/platform/web/dist/
  '';
  passthru = (old.passthru or { }) // {
    inherit frontend;
  };
  meta = old.meta // {
    description = "Chatting private single-operator platform interface";
    mainProgram = "chatting-platform";
  };
})
