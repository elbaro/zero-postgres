{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    openssl
    (perl.withPackages (ps: [ ps.JSON ]))
  ];

  DATABASE_URL = "pg://test:1234@localhost/test";
}
