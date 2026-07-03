{
  description = "Hermetic Linux build of YugabyteDB using yb_build.sh";

  inputs = {
    nixpkgs.url = "https://github.com/NixOS/nixpkgs/archive/nixos-unstable.tar.gz";

    # Keep this in sync with build-support/yugabyte-bash-common-sha1.txt.
    yugabyte-bash-common = {
      url = "https://github.com/yugabyte/yugabyte-bash-common/archive/74793a6e1712ac45dc07cd430da303c95d37f584.tar.gz";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, yugabyte-bash-common }:
    let
      supportedSystems = [ "x86_64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          lib = pkgs.lib;
          pythonPackages = pkgs.python311Packages;

          sys-detection = pythonPackages.buildPythonPackage rec {
            pname = "sys-detection";
            version = "1.3.4";
            pyproject = true;
            src = pkgs.fetchPypi {
              pname = "sys_detection";
              inherit version;
              hash = "sha256-Co0yWhblqpjbHrH5HUY0GIm9yPtse0ZRDJ5WlXunqJU=";
            };
            build-system = [ pythonPackages.setuptools ];
            propagatedBuildInputs = [ autorepr pythonPackages.distro ];
            doCheck = false;
          };

          yugabyte-pycommon = pythonPackages.buildPythonPackage rec {
            pname = "yugabyte-pycommon";
            version = "1.9.15";
            pyproject = true;
            src = pkgs.fetchPypi {
              pname = "yugabyte_pycommon";
              inherit version;
              hash = "sha256-+lWxnV6Mdjnlc7ajUB4/sTxMkqU86SbtgXg8sZcUOAc=";
            };
            build-system = [ pythonPackages.setuptools ];
            doCheck = false;
          };

          autorepr = pythonPackages.buildPythonPackage rec {
            pname = "autorepr";
            version = "0.3.0";
            format = "wheel";
            src = pkgs.fetchurl {
              url = "https://files.pythonhosted.org/packages/f7/a5/339ac779f32f7ed627c84311bb93274906212e5285d5bb80aa5e087161f8/autorepr-0.3.0-py2.py3-none-any.whl";
              hash = "sha256-HZAQ0U+zJdOWHjqnNpJoVWP5fWukovD3NTKfs3QiWZw=";
            };
            doCheck = false;
          };

          pythonEnv = pkgs.python311.withPackages (ps: [
            autorepr
            ps.distro
            ps.overrides
            ps.packaging
            ps.psutil
            ps.ruamel-yaml
            ps.semantic-version
            ps.six
            sys-detection
            yugabyte-pycommon
          ]);

          llvmToolchain = pkgs.symlinkJoin {
            name = "yugabyte-llvm-toolchain-19";
            paths = [
              pkgs.llvmPackages_19.clang
              pkgs.llvmPackages_19.lld
              pkgs.llvmPackages_19.bintools
              pkgs.llvmPackages_19.llvm
            ];
          };

          thirdparty = pkgs.stdenvNoCC.mkDerivation {
            pname = "yugabyte-db-thirdparty";
            version = "20260619071200-fb0ba78d72-ubuntu2404-x86_64-gcc15";
            src = pkgs.fetchurl {
              url = "https://github.com/yugabyte/yugabyte-db-thirdparty/releases/download/v20260619071200-fb0ba78d72-ubuntu2404-x86_64-gcc15/yugabyte-db-thirdparty-v20260619071200-fb0ba78d72-ubuntu2404-x86_64-gcc15.tar.gz";
              hash = "sha256-azLdt+czgrW9eMgYBp/5f4wlCuS6v8q581v/MGHtm6c=";
            };
            nativeBuildInputs = [ pkgs.file pkgs.gnum4 pkgs.gnutar pkgs.gzip pkgs.patchelf ];
            dontConfigure = true;
            dontBuild = true;
            dontPatchELF = true;
            installPhase = ''
              runHook preInstall
              mkdir -p "$out"
              tar -xzf "$src" -C "$out" --strip-components=1

              old_prefix="/opt/yb-build/thirdparty/yugabyte-db-thirdparty-v$version"
              find "$out" -type f \( -name "*.pc" -o -name "*-config" \) -print0 |
                xargs -0 --no-run-if-empty sed -i "s#$old_prefix#$out#g"

              if [ -x "$out/installed/common/bin/bison" ]; then
                mv "$out/installed/common/bin/bison" "$out/installed/common/bin/bison-unwrapped"
                cat > "$out/installed/common/bin/bison" <<EOF
#!${pkgs.bash}/bin/sh
export BISON_PKGDATADIR="$out/installed/common/share/bison"
export PATH="${pkgs.gnum4}/bin:\$PATH"
exec "$out/installed/common/bin/bison-unwrapped" "\$@"
EOF
                chmod +x "$out/installed/common/bin/bison"
              fi

              dynamic_linker="$(cat ${pkgs.stdenv.cc}/nix-support/dynamic-linker)"
              common_rpath="$out/installed/common/lib:$out/installed/uninstrumented/lib:${pkgs.gcc15.cc.lib}/lib"
              while IFS= read -r -d "" file_path; do
                if file "$file_path" | grep -q "ELF"; then
                  old_rpath="$(patchelf --print-rpath "$file_path" 2>/dev/null || true)"
                  new_rpath="$common_rpath"
                  if [ -n "$old_rpath" ]; then
                    old_rpath="''${old_rpath//$old_prefix/$out}"
                    new_rpath="$new_rpath:$old_rpath"
                  fi
                  patchelf --set-rpath "$new_rpath" "$file_path" 2>/dev/null || true
                  if patchelf --print-interpreter "$file_path" >/dev/null 2>&1; then
                    patchelf --set-interpreter "$dynamic_linker" "$file_path" || true
                  fi
                fi
              done < <(find "$out" -type f -print0)

              runHook postInstall
            '';
          };

          sourceGitRev =
            if self ? rev then self.rev
            else "0000000000000000000000000000000000000000";
        in
        {
          default = pkgs.stdenv.mkDerivation {
            pname = "yugabyte-db";
            version = lib.removeSuffix "\n" (builtins.readFile ./version.txt);

            src = self;

            nativeBuildInputs = [
              pkgs.autoconf
              pkgs.automake
              pkgs.bash
              pkgs.bison
              pkgs.cmake
              pkgs.curl
              pkgs.diffutils
              pkgs.file
              pkgs.findutils
              pkgs.flex
              pkgs.gawk
              pkgs.gettext
              pkgs.gnum4
              pkgs.gcc15
              pkgs.git
              pkgs.gnumake
              pkgs.gnugrep
              pkgs.gnused
              pkgs.gnutar
              pkgs.gzip
              pkgs.libtool
              pkgs.ninja
              pkgs.patch
              pkgs.patchelf
              pkgs.perl
              pkgs.pkg-config
              pkgs.python311
              pythonEnv
              pkgs.rsync
              pkgs.unzip
              pkgs.util-linux
              pkgs.which
              pkgs.xz
              pkgs.zip
            ];

            dontConfigure = true;
            dontInstall = true;
            dontStrip = true;

            buildPhase = ''
              runHook preBuild

              export HOME="$TMPDIR/home"
              mkdir -p "$HOME" "$out/build" "$TMPDIR/sysroot/etc"
              cat > "$TMPDIR/sysroot/etc/os-release" <<'EOF'
NAME="Ubuntu"
ID=ubuntu
VERSION_ID="24.04"
VERSION_CODENAME=noble
EOF
              export YB_SYS_DETECTION_BASE_DIR="$TMPDIR/sysroot"

              export LOCALE_ARCHIVE=${pkgs.glibcLocales}/lib/locale/locale-archive
              export LANG=en_US.UTF-8
              export LC_ALL=en_US.UTF-8

              export YB_ALLOW_SOURCE_SNAPSHOT=1
              export YB_VERSION_INFO_GIT_SHA1=${sourceGitRev}

              export YB_BASH_COMMON_DIR="$PWD/build/yugabyte-bash-common"
              mkdir -p "$YB_BASH_COMMON_DIR"
              cp -R ${yugabyte-bash-common}/. "$YB_BASH_COMMON_DIR/"
              chmod -R u+w "$YB_BASH_COMMON_DIR"

              patchShebangs .

              export YB_SKIP_VIRTUALENV=1
              export YB_EXTERNAL_PYTHON_ENV=${pythonEnv}
              export PYTHONPATH="$PWD/python''${PYTHONPATH:+:$PYTHONPATH}"

              export YB_DOWNLOAD_THIRDPARTY=0
              export YB_THIRDPARTY_DIR=${thirdparty}
              export NO_REBUILD_THIRDPARTY=1
              export YB_SKIP_INITIAL_SYS_CATALOG_SNAPSHOT_DOWNLOAD=1
              export YB_SKIP_YSQL_DOCUMENTDB_EXT=1
              export YB_USE_LINUXBREW=0
              export BISON_PKGDATADIR=${thirdparty}/installed/common/share/bison
              export M4=${pkgs.gnum4}/bin/m4
              export YB_PG_EXTRA_CFLAGS="-Wno-error=unused-but-set-variable"
              export YB_LLVM_TOOLCHAIN_DIR=${llvmToolchain}
              export YB_GCC_TOOLCHAIN_DIR=${pkgs.gcc15}
              export YB_RESOLVED_C_COMPILER=${pkgs.gcc15}/bin/gcc
              export YB_RESOLVED_CXX_COMPILER=${pkgs.gcc15}/bin/g++
              export YB_ALLOW_ARBITRARY_BUILD_ROOT=1

              build_root="$out/build/release-gcc15-dynamic-ninja"

              ./yb_build.sh \
                --build-root "$build_root" \
                --gcc15 \
                --ninja \
                --no-ccache \
                -j"$NIX_BUILD_CORES" \
                release \
                daemons \
                initdb \
                --skip-java \
                --skip-pg-parquet \
                --no-odyssey \
                --no-ybc \
                --no-tests

              ln -s build/release-gcc15-dynamic-ninja/bin "$out/bin"
              ln -s build/release-gcc15-dynamic-ninja/postgres "$out/postgres"
              ln -s build/release-gcc15-dynamic-ninja/share "$out/share"

              cat > "$out/nix-build-info.txt" <<EOF
YugabyteDB was built by yb_build.sh.
Build root: $out/build/release-gcc15-dynamic-ninja
Skipped components: Java, pg_parquet, YSQL DocumentDB extension, Odyssey, YBC test controller, tests
Third-party archive: ${thirdparty}
GCC toolchain: ${pkgs.gcc15}
LLVM toolchain: ${llvmToolchain}
EOF

              runHook postBuild
            '';

            meta = {
              description = "YugabyteDB database build produced by yb_build.sh";
              homepage = "https://www.yugabyte.com/";
              license = lib.licenses.asl20;
              platforms = supportedSystems;
            };
          };
        });
    };
}
