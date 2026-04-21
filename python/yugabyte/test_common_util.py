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

from pathlib import Path
import tempfile
import unittest

from yugabyte.common_util import get_yb_src_root_from_build_root


def create_fake_repo(repo_dir: Path) -> Path:
    for subdir in ['.git', 'src', 'java', 'bin', 'build-support']:
        (repo_dir / subdir).mkdir(parents=True, exist_ok=True)
    return repo_dir.resolve()


class TestCommonUtil(unittest.TestCase):
    def test_get_yb_src_root_from_repo_local_build_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            build_root = repo_dir / 'build' / 'debug-clang21-dynamic-ninja'
            build_root.mkdir(parents=True)

            self.assertEqual(get_yb_src_root_from_build_root(str(build_root)), str(repo_dir))

    def test_get_yb_src_root_from_symlinked_build_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')
            repo_build_root = repo_dir / 'build' / 'debug-clang21-dynamic-ninja'
            repo_build_root.mkdir(parents=True)

            symlink_root = tmp_path / 'workspace-link'
            symlink_root.symlink_to(repo_dir)

            build_root = symlink_root / 'build' / 'debug-clang21-dynamic-ninja'
            self.assertEqual(get_yb_src_root_from_build_root(str(build_root)), str(repo_dir))

    def test_get_yb_src_root_from_symlinked_external_build_alias(self) -> None:
        # Regression: when the "__build" directory is itself a symlink to some other path (a
        # common external-build setup), the upward walk from the build root must still recognize
        # the sibling repo via the "__build" suffix. Resolving symlinks up-front strips that
        # suffix and breaks the heuristic.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_dir = create_fake_repo(tmp_path / 'repo')

            external_build_dir = tmp_path / 'external-builds' / 'tmp123'
            external_build_dir.mkdir(parents=True)
            build_root = external_build_dir / 'debug-clang21-dynamic-ninja'
            build_root.mkdir()

            # Alias the external build dir as "<repo>__build" next to the repo.
            build_alias = tmp_path / 'repo__build'
            build_alias.symlink_to(external_build_dir)

            aliased_build_root = build_alias / 'debug-clang21-dynamic-ninja'
            self.assertEqual(
                get_yb_src_root_from_build_root(str(aliased_build_root)), str(repo_dir))


if __name__ == '__main__':
    unittest.main()
