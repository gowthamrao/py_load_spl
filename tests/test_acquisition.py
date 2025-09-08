import pytest
import requests
import requests_mock
from py_load_spl.acquisition import Archive, get_archive_list
from py_load_spl.config import Settings

# A simplified HTML fixture mimicking the structure of the DailyMed page
HTML_FIXTURE = """
<html>
<body>
    <ul class="download">
        <li>
            dm_spl_monthly_update_aug2025.zip [ <a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_monthly_update_aug2025.zip">HTTPS</a> / <a href="ftp://...">FTP</a> ]
            <ul>
                <li>Number of labels: 3,938</li>
                <li>File size: 1.43GB</li>
                <li>MD5 checksum: 05a5c3356b3fa31025294a81ecf8be8f</li>
            </ul>
        </li>
        <li>
            dm_spl_monthly_update_jul2025.zip [ <a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_monthly_update_jul2025.zip">HTTPS</a> / <a href="ftp://...">FTP</a> ]
            <ul>
                <li>Number of labels: 4,379</li>
                <li>File size: 1.56GB</li>
                <li>MD5 checksum: 4a41fe24866f72486c365e95cec550db</li>
            </ul>
        </li>
        <!-- An item without a checksum to ensure it's skipped -->
        <li>
            dm_spl_monthly_update_jun2025.zip [ <a href="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_monthly_update_jun2025.zip">HTTPS</a> / <a href="ftp://...">FTP</a> ]
            <ul>
                <li>File size: 1.67GB</li>
            </ul>
        </li>
    </ul>
</body>
</html>
"""


def test_get_archive_list_success():
    """
    Tests that get_archive_list successfully scrapes and parses the archive page.
    """
    settings = Settings()
    with requests_mock.Mocker() as m:
        m.get(str(settings.fda_source_url), text=HTML_FIXTURE)
        archives = get_archive_list(settings)

    assert len(archives) == 2

    expected_archives = [
        Archive(
            name="dm_spl_monthly_update_aug2025.zip",
            url="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_monthly_update_aug2025.zip",
            checksum="05a5c3356b3fa31025294a81ecf8be8f",
        ),
        Archive(
            name="dm_spl_monthly_update_jul2025.zip",
            url="https://dailymed-data.nlm.nih.gov/public-release-files/dm_spl_monthly_update_jul2025.zip",
            checksum="4a41fe24866f72486c365e95cec550db",
        ),
    ]

    # Sort lists of dicts to ensure comparison is order-independent
    assert sorted(archives, key=lambda x: x['name']) == sorted(expected_archives, key=lambda x: x['name'])


def test_get_archive_list_http_error():
    """
    Tests that get_archive_list raises an exception on HTTP error.
    """
    settings = Settings()
    with requests_mock.Mocker() as m:
        m.get(str(settings.fda_source_url), status_code=500)

        with pytest.raises(requests.exceptions.HTTPError):
            get_archive_list(settings)


def test_get_archive_list_no_archives_found():
    """
    Tests that get_archive_list returns an empty list when no archives are found.
    """
    settings = Settings()
    with requests_mock.Mocker() as m:
        m.get(str(settings.fda_source_url), text="<html><body>No links here</body></html>")

        archives = get_archive_list(settings)
        assert len(archives) == 0


import hashlib
from pathlib import Path
from py_load_spl.acquisition import download_archive


def test_download_archive_success(tmp_path: Path):
    """
    Tests successful download and checksum verification.
    """
    mock_content = b"This is some mock zip file content."
    mock_checksum = hashlib.md5(mock_content).hexdigest()
    archive = Archive(
        name="test_archive.zip",
        url="https://example.com/test_archive.zip",
        checksum=mock_checksum,
    )
    # Use a temporary path for downloads
    settings = Settings(download_path=str(tmp_path))

    with requests_mock.Mocker() as m:
        m.get(archive["url"], content=mock_content, headers={"Content-Length": str(len(mock_content))})
        result_path = download_archive(archive, settings)

    expected_path = tmp_path / archive["name"]
    assert result_path == expected_path
    assert expected_path.exists()
    assert expected_path.read_bytes() == mock_content


def test_download_archive_checksum_mismatch(tmp_path: Path):
    """
    Tests that a checksum mismatch raises an error and cleans up the file.
    """
    mock_content = b"some data"
    wrong_checksum = "thisisnottherightchecksum"
    archive = Archive(
        name="bad_checksum.zip",
        url="https://example.com/bad_checksum.zip",
        checksum=wrong_checksum,
    )
    settings = Settings(download_path=str(tmp_path))
    file_path = tmp_path / archive["name"]

    with requests_mock.Mocker() as m:
        m.get(archive["url"], content=mock_content)
        with pytest.raises(ValueError, match="Checksum mismatch"):
            download_archive(archive, settings)

    assert not file_path.exists()


def test_download_archive_request_error(tmp_path: Path):
    """
    Tests that a request error during download is handled and the file is cleaned up.
    """
    archive = Archive(
        name="error.zip",
        url="https://example.com/error.zip",
        checksum="dummychecksum",
    )
    settings = Settings(download_path=str(tmp_path))
    file_path = tmp_path / archive["name"]

    with requests_mock.Mocker() as m:
        m.get(archive["url"], status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            download_archive(archive, settings)

    assert not file_path.exists()
