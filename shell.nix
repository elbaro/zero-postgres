{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    pkg-config
    openssl
    (perl.withPackages (ps: [ ps.JSON ]))
  ];

  DATABASE_URL = "postgres://test:1234@localhost/test";
}
