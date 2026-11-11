{
  lib,
  python314,
  writeShellApplication,
  src ? ../.,
  version ? (builtins.fromTOML (builtins.readFile ../pyproject.toml)).project.version,
}:
writeShellApplication {
  name = "chatting-worker";
  runtimeInputs = [ python314 ];
  text = ''
    export PYTHONPATH=${src}''${PYTHONPATH:+:$PYTHONPATH}
    exec python -m app.worker.main "$@"
  '';
  derivationArgs = {
    inherit version;
    postCheck = ''
      export PYTHONPYCACHEPREFIX="$TMPDIR/pycache"
      ${python314}/bin/python -m compileall -q ${src}/app
      "$target" --help > /dev/null
    '';
    meta = {
      description = "Chatting task executor with its Python runtime";
      mainProgram = "chatting-worker";
      platforms = lib.platforms.linux;
    };
  };
}
