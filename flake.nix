{
  description = "Oura-to-Postgres - Sync Oura Ring data to PostgreSQL";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      supportedSystems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSystem = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      packages = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          oura-to-postgres = pkgs.python3Packages.buildPythonApplication {
            pname = "oura-to-postgres";
            version = "0.1.0";
            src = ./.;
            format = "pyproject";
            disabled = pkgs.python3Packages.pythonOlder "3.11";

            nativeBuildInputs = with pkgs.python3Packages; [
              hatchling
            ];

            propagatedBuildInputs = with pkgs.python3Packages; [
              httpx
              pg8000
            ];

            meta = {
              description = "Sync Oura Ring health data to PostgreSQL";
              mainProgram = "oura-data-saver";
            };
          };
        in
        {
          default = oura-to-postgres;
          oura-to-postgres = oura-to-postgres;
        }
      );

      apps = forEachSystem (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/oura-data-saver";
        };
      });

      devShells = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            name = "oura-to-postgres-dev";

            packages = with pkgs; [
              (python3.withPackages (ps: with ps; [
                httpx
                pg8000
                hatchling
                # Dev tools
                pytest
                ruff
                mypy
              ]))
              git
            ];

            shellHook = ''
              echo "Oura-to-Postgres dev shell ready"
              echo "Python: $(python --version)"
            '';
          };
        }
      );
    };
}
