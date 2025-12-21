{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    pkg-config
    openssl
    (perl.withPackages (ps: [ ps.JSON ]))
    (python3.withPackages (ps: [ ps.psycopg ]))
  ];

  DATABASE_URL = "postgres://test:1234@localhost/test";
}
