# Copyright (c) YugabyteDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from yugabyte import gen_version_info


def create_fake_repo(repo_dir: Path) -> Path:
    for subdir in ['.git', 'src', 'java', 'bin', 'build-support']:
        (repo_dir / subdir).mkdir(parents=True, exist_ok=True)
    return repo_dir.resolve()


class TestGenVersionInfo(unittest.TestCase):
    def test_resolve_git_repo_dir_from_yb_src_root_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            build_root = tmp_path / 'vm-local' / 'yugabyte-db' / 'build' / \
                'debug-clang21-dynamic-ninja'
            build_root.mkdir(parents=True)

            with patch.dict(os.environ, {'YB_SRC_ROOT': str(repo_dir)}, clear=False):
                self.assertEqual(gen_version_info.resolve_git_repo_dir(str(build_root)),
                                 str(repo_dir))

    def test_resolve_git_repo_dir_from_cmake_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            build_root = tmp_path / 'vm-local' / 'yugabyte-db' / 'build' / \
                'debug-clang21-dynamic-ninja'
            build_root.mkdir(parents=True)
            (build_root / 'CMakeCache.txt').write_text(
                f"CMAKE_HOME_DIRECTORY:INTERNAL={repo_dir}\n",
                encoding='utf-8')

            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(gen_version_info.resolve_git_repo_dir(str(build_root)),
                                 str(repo_dir))

    def test_determine_git_hash_uses_yb_src_root_for_external_build_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            build_root = tmp_path / 'vm-local' / 'yugabyte-db' / 'build' / \
                'debug-clang21-dynamic-ninja'
            build_root.mkdir(parents=True)

            with patch.dict(os.environ, {'YB_SRC_ROOT': str(repo_dir)}, clear=False):
                with patch.object(gen_version_info, 'get_git_sha1', return_value='deadbeef'):
                    self.assertEqual(gen_version_info.determine_git_hash(str(build_root), None),
                                     'deadbeef')

    def test_determine_git_hash_uses_cmake_cache_for_external_build_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            build_root = tmp_path / 'vm-local' / 'yugabyte-db' / 'build' / \
                'debug-clang21-dynamic-ninja'
            build_root.mkdir(parents=True)
            (build_root / 'CMakeCache.txt').write_text(
                f"CMAKE_HOME_DIRECTORY:INTERNAL={repo_dir}\n",
                encoding='utf-8')

            with patch.dict(os.environ, {}, clear=True):
                with patch.object(gen_version_info, 'get_git_sha1', return_value='deadbeef'):
                    self.assertEqual(gen_version_info.determine_git_hash(str(build_root), None),
                                     'deadbeef')


if __name__ == '__main__':
    unittest.main()
