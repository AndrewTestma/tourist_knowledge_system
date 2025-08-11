import unittest
from unittest.mock import patch, MagicMock
import datetime
from typing import List, Dict
from mcp_service import WeatherSyncService


class TestCheckAndUpdateWeather(unittest.TestCase):
    def setUp(self):
        self.mock_self = MagicMock()
        self.entity_id = "AT0010024"
        self.longitude = 116.404
        self.latitude = 39.915
        self.update_interval = 3600

    def test_no_existing_data(self):
        """测试没有现有数据时强制更新"""
        self.mock_self.get_entity_weather.return_value = None

        result = WeatherSyncService.check_and_update_weather(
            self.mock_self, self.entity_id, self.longitude, self.latitude
        )

        self.mock_self.sync_weather_data.assert_called_once_with(
            self.entity_id, self.longitude, self.latitude
        )
        self.mock_self.get_entity_weather.assert_called_with(self.entity_id)

    def test_data_within_interval(self):
        """测试数据在更新间隔内时不更新"""
        old_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1800)
        mock_data = [{"time": old_time.isoformat(), "temp": 25}]
        self.mock_self.get_entity_weather.return_value = mock_data

        result = WeatherSyncService.check_and_update_weather(
            self.mock_self, self.entity_id, self.longitude, self.latitude
        )

        self.mock_self.sync_weather_data.assert_not_called()
        self.assertEqual(result, mock_data)

    def test_data_exceeds_interval(self):
        """测试数据超过更新间隔时更新"""
        old_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=4000)
        mock_data = [{"time": old_time.isoformat(), "temp": 25}]
        self.mock_self.get_entity_weather.side_effect = [mock_data, "new_data"]

        result = WeatherSyncService.check_and_update_weather(
            self.mock_self, self.entity_id, self.longitude, self.latitude
        )

        self.mock_self.sync_weather_data.assert_called_once_with(
            self.entity_id, self.longitude, self.latitude
        )
        self.assertEqual(result, "new_data")

    def test_invalid_time_format(self):
        """测试时间格式无效时强制更新"""
        mock_data = [{"time": "invalid_time", "temp": 25}]
        self.mock_self.get_entity_weather.return_value = mock_data

        result = WeatherSyncService.check_and_update_weather(
            self.mock_self, self.entity_id, self.longitude, self.latitude
        )

        self.mock_self.sync_weather_data.assert_called_once_with(
            self.entity_id, self.longitude, self.latitude
        )
        self.mock_self.logger.warning.assert_called()

    def test_sync_failure(self):
        """测试同步失败时返回旧数据"""
        old_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=4000)
        mock_data = [{"time": old_time.isoformat(), "temp": 25}]
        self.mock_self.get_entity_weather.return_value = mock_data
        self.mock_self.sync_weather_data.return_value = False

        result = WeatherSyncService.check_and_update_weather(
            self.mock_self, self.entity_id, self.longitude, self.latitude
        )

        self.mock_self.sync_weather_data.assert_called_once()
        self.mock_self.logger.warning.assert_called()
        self.assertEqual(result, mock_data)


if __name__ == "__main__":
    unittest.main()
