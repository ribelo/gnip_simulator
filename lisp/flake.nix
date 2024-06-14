{
  description = "lisp env";

  outputs = { self, nixpkgs }: let
    inherit (nixpkgs) lib;
    systems = ["x86_64-linux"];
  in {
    devShells = lib.genAttrs systems (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        default = pkgs.mkShell {
          packages = [
            (pkgs.sbcl.withPackages (ps: [
              ps.alexandria ps.distributions
            ]))
          ];
        };
      });
  };
}
