"""
Tests for the Archive functions
"""
from os.path import exists
import tarfile
from unittest import TestCase
from unittest.mock import patch
from src.archives import tar_and_remove_files
from src.utilities import silent_remove


class TestArchive(TestCase):
    """
    Tests for the Archive functions
    """
    @classmethod
    def tearDownClass(cls):
        """
        Clean up files
        """
        silent_remove('./testfile1.txt')
        silent_remove('./testfile2.txt')
        silent_remove('./test.tar')

    @patch('src.utilities.write_to_logs')
    def test_tar_and_remove_files(self, _):
        """
        Test that:
            * the tar is created
            * the original files are removed
            * untarring creates the files, no folders
        """
        with open('./testfile1.txt', 'w') as testfile1:
            testfile1.write('Things and stuff')

        with open('./testfile2.txt', 'w') as testfile2:
            testfile2.write('Stuff and things')

        files_to_tar = ['./testfile1.txt', './testfile2.txt']
        tar_and_remove_files('test', './', files_to_tar, None)

        self.assertTrue(exists('./test.tar'))
        self.assertFalse(exists('./testfile1.txt'))
        self.assertFalse(exists('./testfile2.txt'))
        self.assertTrue(tarfile.is_tarfile('./test.tar'))

        with tarfile.open('./test.tar') as test_tar:
            test_tar.extractall('./')

        self.assertTrue(exists('./testfile1.txt'))
        self.assertTrue(exists('./testfile2.txt'))
