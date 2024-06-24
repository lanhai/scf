<?php

namespace Scf\Util;

use JetBrains\PhpStorm\Pure;

/**
 * Date helper.
 */
class Date {

    // Second amounts for various time increments
    const YEAR = 31556926;
    const MONTH = 2629744;
    const WEEK = 604800;
    const DAY = 86400;
    const HOUR = 3600;
    const MINUTE = 60;
    // Available formats for Date::months()
    const MONTHS_LONG = '%B';
    const MONTHS_SHORT = '%b';

    /**
     * Default timestamp format for formatted_time
     * @var  string
     */
    public static string $timestamp_format = 'Y-m-d H:i:s';

    /**
     * Timezone for formatted_time
     * @link http://uk2.php.net/manual/en/timezones.php
     * @var  string
     */
    public static string $timezone;

    /**
     * Returns the offset (in seconds) between two time zones. Use this to
     * display dates to users in different time zones.
     *
     *     $seconds = Date::offset('America/Chicago', 'GMT');
     *
     * [!!] A list of time zones that PHP supports can be found at
     * <http://php.net/timezones>.
     *
     * @param string $remote timezone that to find the offset of
     * @param string $local timezone used as the baseline
     * @param mixed $now UNIX timestamp or date string
     * @return  integer
     * @throws \Exception
     */
    public static function offset($remote, $local = null, $now = null): int {
        if ($local === null) {
            // Use the default timezone
            $local = date_default_timezone_get();
        }

        if (is_int($now)) {
            // Convert the timestamp into a string
            $now = date(\DateTime::RFC2822, $now);
        }

        // Create timezone objects
        $zone_remote = new \DateTimeZone($remote);
        $zone_local = new \DateTimeZone($local);

        // Create date objects from timezones
        $time_remote = new \DateTime($now, $zone_remote);
        $time_local = new \DateTime($now, $zone_local);

        // Find the offset
        $offset = $zone_remote->getOffset($time_remote) - $zone_local->getOffset($time_local);

        return $offset;
    }

    /**
     * Number of seconds in a minute, incrementing by a step. Typically used as
     * a shortcut for generating a list that can used in a form.
     *
     *     $seconds = Date::seconds(); // 01, 02, 03, ..., 58, 59, 60
     *
     * @param integer $step amount to increment each step by, 1 to 30
     * @param integer $start start value
     * @param integer $end end value
     * @return  array   A mirrored (foo => foo) array from 1-60.
     */
    public static function seconds($step = 1, $start = 0, $end = 60) {
        // Always integer
        $step = (int)$step;

        $seconds = array();

        for ($i = $start; $i < $end; $i += $step) {
            $seconds[$i] = sprintf('%02d', $i);
        }

        return $seconds;
    }

    /**
     * Number of minutes in an hour, incrementing by a step. Typically used as
     * a shortcut for generating a list that can be used in a form.
     *
     *     $minutes = Date::minutes(); // 05, 10, 15, ..., 50, 55, 60
     *
     * @param integer $step amount to increment each step by, 1 to 30
     * @return  array   A mirrored (foo => foo) array from 1-60.
     * @uses    Date::seconds
     */
    public static function minutes($step = 5) {
        // Because there are the same number of minutes as seconds in this set,
        // we choose to re-use seconds(), rather than creating an entirely new
        // function. Shhhh, it's cheating! ;) There are several more of these
        // in the following methods.
        return Date::seconds($step);
    }

    /**
     * Number of hours in a day. Typically used as a shortcut for generating a
     * list that can be used in a form.
     *
     *     $hours = Date::hours(); // 01, 02, 03, ..., 10, 11, 12
     *
     * @param integer $step amount to increment each step by
     * @param boolean $long use 24-hour time
     * @param integer $start the hour to start at
     * @return  array   A mirrored (foo => foo) array from start-12 or start-23.
     */
    public static function hours($step = 1, $long = false, $start = null) {
        // Default values
        $step = (int)$step;
        $long = (bool)$long;
        $hours = array();

        // Set the default start if none was specified.
        if ($start === null) {
            $start = ($long === false) ? 1 : 0;
        }

        $hours = array();

        // 24-hour time has 24 hours, instead of 12
        $size = ($long === true) ? 23 : 12;

        for ($i = $start; $i <= $size; $i += $step) {
            $hours[$i] = (string)$i;
        }

        return $hours;
    }

    /**
     * Returns AM or PM, based on a given hour (in 24 hour format).
     *
     *     $type = Date::ampm(12); // PM
     *     $type = Date::ampm(1);  // AM
     *
     * @param integer $hour number of the hour
     * @return  string
     */
    public static function ampm($hour) {
        // Always integer
        $hour = (int)$hour;

        return ($hour > 11) ? 'PM' : 'AM';
    }

    /**
     * Adjusts a non-24-hour number into a 24-hour number.
     *
     *     $hour = Date::adjust(3, 'pm'); // 15
     *
     * @param integer $hour hour to adjust
     * @param string $ampm AM or PM
     * @return  string
     */
    public static function adjust($hour, $ampm) {
        $hour = (int)$hour;
        $ampm = strtolower($ampm);

        switch ($ampm) {
            case 'am':
                if ($hour == 12) {
                    $hour = 0;
                }
                break;
            case 'pm':
                if ($hour < 12) {
                    $hour += 12;
                }
                break;
        }

        return sprintf('%02d', $hour);
    }

    /**
     * Number of days in a given month and year. Typically used as a shortcut
     * for generating a list that can be used in a form.
     *
     *     Date::days(4, 2010); // 1, 2, 3, ..., 28, 29, 30
     *
     * @param integer $month number of month
     * @param bool|int $year number of year to check month, defaults to the current year
     * @return  array   A mirrored (foo => foo) array of the days.
     */
    public static function days($month, $year = false) {
        static $months;

        if ($year === false) {
            // Use the current year by default
            $year = date('Y');
        }

        // Always integers
        $month = (int)$month;
        $year = (int)$year;

        // We use caching for months, because time functions are used
        if (empty($months[$year][$month])) {
            $months[$year][$month] = array();

            // Use date to find the number of days in the given month
            $total = date('t', mktime(1, 0, 0, $month, 1, $year)) + 1;

            for ($i = 1; $i < $total; $i++) {
                $months[$year][$month][$i] = (string)$i;
            }
        }

        return $months[$year][$month];
    }

    /**
     * Number of months in a year. Typically used as a shortcut for generating
     * a list that can be used in a form.
     *
     * By default a mirrored array of $month_number => $month_number is returned
     *
     *     Date::months();
     *     // aray(1 => 1, 2 => 2, 3 => 3, ..., 12 => 12)
     *
     * But you can customise this by passing in either Date::MONTHS_LONG
     *
     *     Date::months(Date::MONTHS_LONG);
     *     // array(1 => 'January', 2 => 'February', ..., 12 => 'December')
     *
     * Or Date::MONTHS_SHORT
     *
     *     Date::months(Date::MONTHS_SHORT);
     *     // array(1 => 'Jan', 2 => 'Feb', ..., 12 => 'Dec')
     *
     * @param string|null $format The format to use for months
     * @return  array   An array of months based on the specified format
     * @uses    Date::hours
     */
    public static function months(string $format = null): array {
        $months = array();

        if ($format === Date::MONTHS_LONG or $format === Date::MONTHS_SHORT) {
            for ($i = 1; $i <= 12; ++$i) {
                $months[$i] = strftime($format, mktime(0, 0, 0, $i, 1));
            }
        } else {
            $months = Date::hours();
        }

        return $months;
    }

    /**
     * Returns an array of years between a starting and ending year. By default,
     * the the current year - 5 and current year + 5 will be used. Typically used
     * as a shortcut for generating a list that can be used in a form.
     *
     *     $years = Date::years(2000, 2010); // 2000, 2001, ..., 2009, 2010
     *
     * @param bool|int $start starting year (default is current year - 5)
     * @param bool|int $end ending year (default is current year + 5)
     * @return  array
     */
    public static function years($start = false, $end = false) {
        // Default values
        $start = ($start === false) ? (date('Y') - 5) : (int)$start;
        $end = ($end === false) ? (date('Y') + 5) : (int)$end;

        $years = array();

        for ($i = $start; $i <= $end; $i++) {
            $years[$i] = (string)$i;
        }

        return $years;
    }

    /**
     * Returns time difference between two timestamps, in human readable format.
     * If the second timestamp is not given, the current time will be used.
     * Also consider using [Date::fuzzy_span] when displaying a span.
     *
     *     $span = Date::span(60, 182, 'minutes,seconds'); // array('minutes' => 2, 'seconds' => 2)
     *     $span = Date::span(60, 182, 'minutes'); // 2
     *
     * @param integer $remote timestamp to find the span of
     * @param integer $local timestamp to use as the baseline
     * @param string $output formatting string
     * @return  string   when only a single output is requested
     * @return  array    associative list of all outputs requested
     */
    public static function span($remote, $local = null, $output = 'years,months,weeks,days,hours,minutes,seconds') {
        // Normalize output
        $output = trim(strtolower((string)$output));

        if (!$output) {
            // Invalid output
            return false;
        }

        // Array with the output formats
        $output = preg_split('/[^a-z]+/', $output);

        // Convert the list of outputs to an associative array
        $output = array_combine($output, array_fill(0, count($output), 0));

        // Make the output values into keys
        extract(array_flip($output), EXTR_SKIP);

        if ($local === null) {
            // Calculate the span from the current time
            $local = time();
        }

        // Calculate timespan (seconds)
        $timespan = abs($remote - $local);

        if (isset($output['years'])) {
            $timespan -= Date::YEAR * ($output['years'] = (int)floor($timespan / Date::YEAR));
        }

        if (isset($output['months'])) {
            $timespan -= Date::MONTH * ($output['months'] = (int)floor($timespan / Date::MONTH));
        }

        if (isset($output['weeks'])) {
            $timespan -= Date::WEEK * ($output['weeks'] = (int)floor($timespan / Date::WEEK));
        }

        if (isset($output['days'])) {
            $timespan -= Date::DAY * ($output['days'] = (int)floor($timespan / Date::DAY));
        }

        if (isset($output['hours'])) {
            $timespan -= Date::HOUR * ($output['hours'] = (int)floor($timespan / Date::HOUR));
        }

        if (isset($output['minutes'])) {
            $timespan -= Date::MINUTE * ($output['minutes'] = (int)floor($timespan / Date::MINUTE));
        }

        // Seconds ago, 1
        if (isset($output['seconds'])) {
            $output['seconds'] = $timespan;
        }

        if (count($output) === 1) {
            // Only a single output was requested, return it
            return array_pop($output);
        }

        // Return array
        return $output;
    }

    /**
     * Returns the difference between a time and now in a "fuzzy" way.
     * Displaying a fuzzy time instead of a date is usually faster to read and understand.
     *
     *     $span = Date::fuzzy_span(time() - 10); // "moments ago"
     *     $span = Date::fuzzy_span(time() + 20); // "in moments"
     *
     * A second parameter is available to manually set the "local" timestamp,
     * however this parameter shouldn't be needed in normal usage and is only
     * included for unit tests
     *
     * @param integer $timestamp "remote" timestamp
     * @param integer $local_timestamp "local" timestamp, defaults to time()
     * @return  string
     */
    public static function fuzzySpan($timestamp, $local_timestamp = null) {
        $local_timestamp = ($local_timestamp === null) ? time() : (int)$local_timestamp;

        // Determine the difference in seconds
        $offset = abs($local_timestamp - $timestamp);

        if ($offset <= Date::MINUTE) {
            $span = 'moments';
        } elseif ($offset < (Date::MINUTE * 20)) {
            $span = 'a few minutes';
        } elseif ($offset < Date::HOUR) {
            $span = 'less than an hour';
        } elseif ($offset < (Date::HOUR * 4)) {
            $span = 'a couple of hours';
        } elseif ($offset < Date::DAY) {
            $span = 'less than a day';
        } elseif ($offset < (Date::DAY * 2)) {
            $span = 'about a day';
        } elseif ($offset < (Date::DAY * 4)) {
            $span = 'a couple of days';
        } elseif ($offset < Date::WEEK) {
            $span = 'less than a week';
        } elseif ($offset < (Date::WEEK * 2)) {
            $span = 'about a week';
        } elseif ($offset < Date::MONTH) {
            $span = 'less than a month';
        } elseif ($offset < (Date::MONTH * 2)) {
            $span = 'about a month';
        } elseif ($offset < (Date::MONTH * 4)) {
            $span = 'a couple of months';
        } elseif ($offset < Date::YEAR) {
            $span = 'less than a year';
        } elseif ($offset < (Date::YEAR * 2)) {
            $span = 'about a year';
        } elseif ($offset < (Date::YEAR * 4)) {
            $span = 'a couple of years';
        } elseif ($offset < (Date::YEAR * 8)) {
            $span = 'a few years';
        } elseif ($offset < (Date::YEAR * 12)) {
            $span = 'about a decade';
        } elseif ($offset < (Date::YEAR * 24)) {
            $span = 'a couple of decades';
        } elseif ($offset < (Date::YEAR * 64)) {
            $span = 'several decades';
        } else {
            $span = 'a long time';
        }

        if ($timestamp <= $local_timestamp) {
            // This is in the past
            return $span . ' ago';
        } else {
            // This in the future
            return 'in ' . $span;
        }
    }

    /**
     * Converts a UNIX timestamp to DOS format. There are very few cases where
     * this is needed, but some binary formats use it (eg: zip files.)
     * Converting the other direction is done using {@link Date::dos2unix}.
     *
     *     $dos = Date::unix2dos($unix);
     *
     * @param bool|int $timestamp UNIX timestamp
     * @return  integer
     */
    public static function unix2dos($timestamp = false) {
        $timestamp = ($timestamp === false) ? getdate() : getdate($timestamp);

        if ($timestamp['year'] < 1980) {
            return (1 << 21 | 1 << 16);
        }

        $timestamp['year'] -= 1980;

        // What voodoo is this? I have no idea... Geert can explain it though,
        // and that's good enough for me.
        return ($timestamp['year'] << 25 | $timestamp['mon'] << 21 |
            $timestamp['mday'] << 16 | $timestamp['hours'] << 11 |
            $timestamp['minutes'] << 5 | $timestamp['seconds'] >> 1);
    }

    /**
     * Converts a DOS timestamp to UNIX format.There are very few cases where
     * this is needed, but some binary formats use it (eg: zip files.)
     * Converting the other direction is done using {@link Date::unix2dos}.
     *
     *     $unix = Date::dos2unix($dos);
     *
     * @param bool|int $timestamp DOS timestamp
     * @return  integer
     */
    public static function dos2unix($timestamp = false) {
        $sec = 2 * ($timestamp & 0x1f);
        $min = ($timestamp >> 5) & 0x3f;
        $hrs = ($timestamp >> 11) & 0x1f;
        $day = ($timestamp >> 16) & 0x1f;
        $mon = ($timestamp >> 21) & 0x0f;
        $year = ($timestamp >> 25) & 0x7f;

        return mktime($hrs, $min, $sec, $mon, $day, $year + 1980);
    }

    /**
     * Returns a date/time string with the specified timestamp format
     *
     *     $time = Date::formatted_time('5 minutes ago');
     *
     * @link    http://www.php.net/manual/datetime.construct
     * @param string $datetime_str datetime string
     * @param string $timestamp_format timestamp format
     * @param string $timezone timezone identifier
     * @return  string
     */
    public static function formattedTime($datetime_str = 'now', $timestamp_format = null, $timezone = null) {
        $timestamp_format = ($timestamp_format == null) ? Date::$timestamp_format : $timestamp_format;
        $timezone = ($timezone === null) ? Date::$timezone : $timezone;

        $tz = new \DateTimeZone($timezone ? $timezone : date_default_timezone_get());
        $time = new \DateTime($datetime_str, $tz);

        if ($time->getTimeZone()->getName() !== $tz->getName()) {
            $time->setTimeZone($tz);
        }

        return $time->format($timestamp_format);
    }

    /**
     *获取日期段日历
     * @param $start
     * @param $end
     * @param string $format
     * @param int $weekdayFormat 星期几格式
     * @return array
     */
    public static function calendar($start, $end, string $format = "Ymd", int $weekdayFormat = 1): array {
        $startDay = date('Y-m-d', strtotime($start));
        $endDay = date('Y-m-d', strtotime($end));

        $days = (strtotime($endDay) - strtotime($startDay)) / 86400;
        $list = [];
        $weekdays = ['星期日', '星期一', '星期二', '星期三', '星期四', '星期五', '星期六'];
        for ($i = 0; $i <= $days; $i++) {
            $value = [];
            $value['day'] = self::day(date('Y-m-d', strtotime($startDay . '+' . $i . ' day')), $format);
            $weekDay = date('w', strtotime($startDay . '+' . $i . ' day'));
            if ($weekdayFormat == 2) {
                $value['week_day'] = $weekdays[intval($weekDay)];
            } else {
                $value['week_day'] = $weekDay;
            }
            $list[] = $value;
        }
        return $list;
    }

    /**
     * 秒转个性化时间
     * @param int $seconds
     * @param string $unitStyle
     * @return string
     */
    public static function secondsHumanize(int $seconds, string $unitStyle = "cn"): string {
        $day = 0;
        $result = "";
        $lang = [
            'cn' => [
                "{day}" => '天',
                "{hour}" => '小时',
                "{min}" => '分',
                "{sec}" => '秒'
            ],
            'en' => [
                "{day}" => 'days',
                "{hour}" => 'hours',
                "{min}" => 'minutes',
                "{sec}" => 'seconds'
            ]
        ];
        $seconds = max(0, $seconds);
        if (!$seconds) {
            goto format;
        }
        if ($seconds > 86400) {
            $day = floor($seconds / 86400);
            $seconds = $seconds % 86400;
            $result .= $day . "{day}";
        }
        $hour = floor($seconds / 3600);
        if ($hour) {
            $seconds = $seconds % 3600;
        }
        if ($day > 0 || $hour > 0) {
            $result .= $hour . "{hour}";
        }
        $min = floor($seconds / 60);
        if ($min) {
            $seconds = $seconds % 60;
        }
        if ($day > 0 || $hour > 0 || $min > 0) {
            $result .= $min . "{min}";
        }
        format:
        $unit = $lang[$unitStyle];
        $result .= $seconds . "{sec}";
        foreach ($unit as $key => $val) {
            $result = str_replace($key, $val, $result);
        }
        return $result;
    }

    /**
     * 获取时间线
     * @param $start
     * @param $end
     * @param string $unit
     * @param string $format
     * @param int $weekdayFormat
     * @return array
     */
    public static function timeline($start, $end, string $unit = 'hour', string $format = "Y-m-d H:i:s", int $weekdayFormat = 1): array {
        $startTime = date('Y-m-d H:i:s', strtotime($start));
        $endTime = date('Y-m-d H:i:s', strtotime($end));
        switch ($unit) {
            case 'hour':
                $total = intval((strtotime($endTime) - strtotime($startTime)) / 3600);
                $list = [];
                for ($i = 0; $i <= $total; $i++) {
                    $list[] = self::hour(date('Y-m-d H:i:s', strtotime($startTime . '+' . $i . ' hour')), $format);
                }
                break;
            case 'day':
                return self::calendar($start, $end, $format, 1);
                break;
            case 'month':
                $total = self::getMonthNum(date('Y-m-d', strtotime($start)), date('Y-m-d', strtotime($end)));
                $list = [];
                for ($i = 0; $i <= $total; $i++) {
                    $list[] = self::month(date('Y-m', strtotime($startTime . '+' . $i . ' month')), $format);
                }
                break;
        }

        return $list;
    }

    /**
     * 返回上月开始&结束时间戳
     * @return array
     */
    public static function lastMonth() {
        $lastmonth_start = mktime(0, 0, 0, date("m") - 1, 1, date("Y"));
        $lastmonth_end = mktime(23, 59, 59, date("m"), 0, date("Y"));
        return ['start' => $lastmonth_start, 'end' => $lastmonth_end];
    }

    /**
     * 返回本月开始&结束时间戳
     * @return array
     */
    public static function thisMonth() {
        $thismonth_start = mktime(0, 0, 0, date("m"), 1, date("Y"));
        $thismonth_end = mktime(23, 59, 59, date("m"), date("t"), date("Y"));
        return ['start' => $thismonth_start, 'end' => $thismonth_end];
    }

    /**
     * 当前周
     * @param $firstDay
     * @return int
     */
    public static function currentWeek($firstDay = '') {
        if (!$firstDay) {
            $firstDay = date('Y-01-01');
        }
        $year = date('Y', strtotime($firstDay));
        $month = date('m', strtotime($firstDay));
        $day = date('d', strtotime($firstDay));
        $time_chuo_of_first_day = mktime(0, 0, 0, $month, $day, $year);
        //今天的时间戳
        $month = date('m'); //获取月
        $day = date('d'); //获取日 d
        $year = date('Y'); //获取年 Y
        $time_chuo_of_current_day = mktime(0, 0, 0, $month, $day, $year);
        $cha = ($time_chuo_of_current_day - $time_chuo_of_first_day) / 60 / 60 / 24;
        $zhou = (int)(($cha) / 7 + 1);
        return $zhou;
    }

    /**
     * 计算一个时间戳剩余多少天
     * @param $timestamp
     * @return float
     */
    public static function leftDays($timestamp): float {
        return floor(($timestamp - time()) / (24 * 60 * 60));
    }

    /**
     * 获得今天的格式化日期
     * @param string $format
     * @return array|string
     */
    public static function today($format = "Ymd") {
        $today = date('Y-m-d');
        return self::day($today, $format);
    }

    /**
     * 获得昨天的格式化日期
     * @param string $format
     * @return array|string
     */
    public static function yesterday($format = "Ymd") {
        $lastDay = date('Y-m-d', strtotime('-1 day'));
        return self::day($lastDay, $format);

    }

    /**
     * 获得明天天的格式化日期
     * @param string $format
     * @return array|string
     */
    public static function tomorrow($format = "Ymd") {
        $lastDay = date('Y-m-d', strtotime('+1 day'));
        return self::day($lastDay, $format);

    }

    /**
     * 获得N天之前的格式化日期
     * @param int $days
     * @param string $format
     * @param string $start
     * @return array|string
     */
    public static function pastDay($days = 1, $format = "Ymd", $start = '') {
        if ($start) {
            $lastDay = date('Y-m-d', strtotime($start . '-' . $days . ' day'));
        } else {
            $lastDay = date('Y-m-d', strtotime('-' . $days . ' day'));
        }
        return self::day($lastDay, $format);
    }

    /**
     * 获取过去N天的列表
     * @param int $days
     * @param string $format
     * @return array
     */
    public static function pastDays($days = 1, $format = "Ymd") {
        $list = [];
        for ($i = $days; $i > 0; $i--) {
            $list[] = self::day(date('Y-m-d', strtotime('-' . $i . ' day')), $format);
        }
        return $list;
    }

    /**
     * 获得N天之后的格式化日期
     * @param int $days
     * @param string $format
     * @return array|string
     */
    public static function nextDay($days = 1, $format = "Ymd") {
        $lastDay = date('Y-m-d', strtotime('+' . $days . ' day'));
        return self::day($lastDay, $format);
    }

    /**
     * 获取将来N天的列表
     * @param int $days
     * @param string $format
     * @return array
     */
    public static function nextDays($days = 1, $format = "Ymd") {
        $list = [];
        for ($i = 1; $i <= $days; $i++) {
            $list[] = self::day(date('Y-m-d', strtotime('+' . $i . ' day')), $format);
        }
        return $list;
    }

    /**
     * 过去N月格式化
     * @param int $month
     * @param string $format
     * @param bool $calendar 是否返回日历
     * @return array|string
     */
    public static function pastMonth($month = 1, $format = "Y-m", $calendar = false) {
        $day = date('Y-m-01', strtotime('-' . $month . ' month'));
        if ($calendar) {
            $days = date('t', strtotime($day));
            $list = [];
            for ($i = 0; $i < $days; $i++) {
                $list[] = self::day(date('Y-m-d', strtotime($day . '+' . $i . ' day')), $format);
            }
            return $list;
        }
        return self::month($day, $format);
    }

    /**
     * 过去N月的列表
     * @param int $month
     * @param string $format
     * @param bool $calendar 是否返回日历
     * @return array
     */
    #[Pure(true)]
    public static function pastMonths(int $month = 1, string $format = "Y-m", bool $calendar = false): array {
        $list = [];
        $dayList = [];
        for ($i = $month; $i > 0; $i--) {
            $list[] = self::month(date($format, strtotime('-' . $i . ' month')), $format);
            if ($calendar) {
                $startDay = date('Y-m-01', strtotime('-' . $i . ' month'));
                $days = date('t', strtotime($startDay));
                for ($d = 0; $d < $days; $d++) {
                    $dayList[] = self::day(date('Y-m-d', strtotime($startDay . '+' . $d . ' day')), $format);
                }
            }
        }
        if ($calendar) {
            return $dayList;
        }
        return $list;
    }

    /**
     * 未来N月格式化
     * @param int $month
     * @param string $format
     * @param bool $calendar 是否返回日历
     * @return array|string
     */
    public static function nextMonth($month = 1, $format = "Y-m", $calendar = false) {
        $day = date('Y-m-01', strtotime('+' . $month . ' month'));
        if ($calendar) {
            $days = date('t', strtotime($day));
            $list = [];
            for ($i = 0; $i < $days; $i++) {
                $list[] = self::day(date('Y-m-d', strtotime($day . '+' . $i . ' day')), $format);
            }
            return $list;
        }
        return self::month($day, $format);
    }

    /**
     * 未来N月的列表
     * @param int $month
     * @param string $format
     * @param bool $calendar 是否返回日历
     * @return array
     */
    public static function nextMonths($month = 1, $format = "Y-m", $calendar = false) {
        $list = [];
        $dayList = [];
        for ($i = 1; $i <= $month; $i++) {
            $list[] = self::month(date($format, strtotime('+' . $i . ' month')), $format);
            if ($calendar) {
                $startDay = date('Y-m-01', strtotime('+' . $i . ' month'));
                $days = date('t', strtotime($startDay));
                for ($d = 0; $d < $days; $d++) {
                    $dayList[] = self::day(date('Y-m-d', strtotime($startDay . '+' . $d . ' day')), $format);
                }
            }
        }
        if ($calendar) {
            return $dayList;
        }
        return $list;
    }

    /**
     * @return 相差的月份数量
     * @var date2 日期2
     * @var tags 年月日之间的分隔符标记,默认为'-'
     * @var date1日期1
     * @example:
     * $date1 = "2003-08-11";
     * $date2 = "2008-11-06";
     * $monthNum = getMonthNum( $date1 , $date2 );
     * echo $monthNum;
     */
    private static function getMonthNum($date1, $date2, $tags = '-') {
        $date1 = explode($tags, $date1);
        $date2 = explode($tags, $date2);
        return abs($date1[0] - $date2[0]) * 12 + abs($date1[1] - $date2[1]);
    }

    /**
     * 返回单位:月
     * @param $date
     * @param $format
     * @return array|string
     */
    private static function month($datetime, $format = "Ym") {
        if ($format == 'timestamp') {
            $date = date('Y-m', strtotime($datetime));
            $start = strtotime(date('Y-m-01 00:00:00', strtotime($datetime)));
            $days = date('t', strtotime($date));
            $end = strtotime(date('Y-m-d 23:59:59', strtotime(date('Y-m-d', $start) . '+' . ($days - 1) . ' day')));
            return compact('date', 'start', 'end');
        }
        return date($format, strtotime($datetime));
    }

    /**
     * 返回单位:时
     * @param $datetime
     * @param $format
     * @return array|string
     */
    private static function hour($datetime, $format = "Ymd H:00:00") {
        if ($format == 'timestamp') {
            $date = date('Y-m-d H:00:00', strtotime($datetime));
            $start = strtotime(date('Y-m-d H:00:00', strtotime($datetime)));
            $end = strtotime(date('Y-m-d H:59:59', strtotime($datetime)));
            return compact('date', 'start', 'end');
        }
        return date($format, strtotime($datetime));
    }

    /**
     * 返回单位:日
     * @param $datetime
     * @param $format
     * @return array|string
     */
    public static function day($datetime, $format = "Ymd") {
        if ($format == 'timestamp') {
            $date = date('Y-m-d', strtotime($datetime));
            $start = strtotime(date('Y-m-d 00:00:00', strtotime($datetime)));
            $end = strtotime(date('Y-m-d 23:59:59', strtotime($datetime)));
            return compact('date', 'start', 'end');
        }
        return date($format, strtotime($datetime));
    }

    /**
     * 获得过去某天的日期
     * @param $format
     * @return int Ymd
     */
    public static function leftday($days, $format = 'Ymd') {
        $lastDay = date('Y-m-d', strtotime('-' . $days . ' day'));
        return self::day($lastDay, $format);
    }

    public static function rightday($days, $format = 'Ymd') {
        $lastDay = date('Y-m-d', strtotime('+' . $days . ' day'));
        return self::day($lastDay, $format);
    }

    /**
     * 时间转换函数(把时间显示人性化)
     */
    public static function format($time) {
        $rtime = date("m-d H:i:s", $time);
        $hourtime = date("H:i:s", $time);
        $htime = date("H:i:s", $time);
        $time = time() - $time;
        if ($time < 60) {
            $str = '刚刚';
        } elseif ($time < 60 * 60) {
            $min = floor($time / 60);
            $str = $min . '分钟前';
        } elseif ($time < 60 * 60 * 24) {
            $h = floor($time / (60 * 60));
            $thisHour = date('H');
            if ($thisHour - $h >= 0) {
                $str = $h . '小时前 '; //.$htime;
            } else {
                $str = '昨天 ' . $hourtime;
            }
        } elseif ($time < 60 * 60 * 24 * 3) {
            $d = floor($time / (60 * 60 * 24));
            if ($d == 1) {
                $str = '昨天 ' . $hourtime;
            } else {
                $str = '前天 ' . $hourtime;
            }
        } else {
            $str = $rtime;
        }
        return $str;
    }

}
