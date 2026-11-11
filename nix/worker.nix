{
  lib,
  python314,
  writeShellApplication,
  fetchurl,
  src ? ../.,
  version ? (builtins.fromTOML (builtins.readFile ../pyproject.toml)).project.version,
}:
let
  lockedCroniter = builtins.head (
    builtins.filter (package: package.name == "croniter")
      (builtins.fromTOML (builtins.readFile ../uv.lock)).package
  );
  python = python314.withPackages (ps: [
    (ps.buildPythonPackage {
      pname = "croniter";
      version = "6.2.2";
      format = "wheel";
      src = fetchurl {
        url = "https://files.pythonhosted.org/packages/d0/39/783980e78cb92c2d7bdb1fc7dbc86e94ccc6d58224d76a7f1f51b6c51e30/croniter-6.2.2-py3-none-any.whl";
        hash = "sha256-pdF7EGCXTTYlHqT684gjPsqKzw0Jy9ktNfTErI8nmWA=";
      };
      dependencies = [ ps.python-dateutil ];
      pythonImportsCheck = [ "croniter" ];
    })
  ]);
in
assert lib.assertMsg (
  lockedCroniter.version == "6.2.2"
) "Update the packaged croniter wheel and hash to match uv.lock";
writeShellApplication {
  name = "chatting-worker";
  runtimeInputs = [ python ];
  text = ''
    export PYTHONPATH=${src}''${PYTHONPATH:+:$PYTHONPATH}
    exec python -m app.worker.main "$@"
  '';
  derivationArgs = {
    inherit version;
    postCheck = ''
      export PYTHONPYCACHEPREFIX="$TMPDIR/pycache"
      ${python}/bin/python -m compileall -q ${src}/app
      ${python}/bin/python -c 'import importlib.metadata; assert importlib.metadata.version("croniter") == "6.2.2"'
      "$target" --help > /dev/null
    '';
    meta = {
      description = "Chatting task executor with its Python runtime and dependencies";
      mainProgram = "chatting-worker";
      platforms = lib.platforms.linux;
    };
  };
}
