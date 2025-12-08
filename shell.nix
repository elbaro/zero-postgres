{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    openssl
  ];

  DATABASE_URL = "pg://test:1234@localhost/test";
}
