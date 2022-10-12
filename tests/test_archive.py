"""
Tests for the Archive functions
"""
from os.path import exists
import tarfile
from unittest import TestCase
from unittest.mock import patch
from src.archive import tar_and_remove_files
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
            
            import os
            
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(test_tar, "./")

        self.assertTrue(exists('./testfile1.txt'))
        self.assertTrue(exists('./testfile2.txt'))
