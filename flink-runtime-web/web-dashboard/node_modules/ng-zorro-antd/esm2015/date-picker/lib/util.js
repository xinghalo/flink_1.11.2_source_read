/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/** @type {?} */
const defaultDisabledTime = {
    /**
     * @return {?}
     */
    nzDisabledHours() {
        return [];
    },
    /**
     * @return {?}
     */
    nzDisabledMinutes() {
        return [];
    },
    /**
     * @return {?}
     */
    nzDisabledSeconds() {
        return [];
    }
};
/**
 * @param {?} value
 * @param {?} disabledTime
 * @return {?}
 */
export function getTimeConfig(value, disabledTime) {
    /** @type {?} */
    let disabledTimeConfig = disabledTime ? disabledTime(value && value.nativeDate) : (/** @type {?} */ ({}));
    disabledTimeConfig = Object.assign({}, defaultDisabledTime, disabledTimeConfig);
    return disabledTimeConfig;
}
/**
 * @param {?} value
 * @param {?} disabledTimeConfig
 * @return {?}
 */
export function isTimeValidByConfig(value, disabledTimeConfig) {
    /** @type {?} */
    let invalidTime = false;
    if (value) {
        /** @type {?} */
        const hour = value.getHours();
        /** @type {?} */
        const minutes = value.getMinutes();
        /** @type {?} */
        const seconds = value.getSeconds();
        /** @type {?} */
        const disabledHours = disabledTimeConfig.nzDisabledHours();
        if (disabledHours.indexOf(hour) === -1) {
            /** @type {?} */
            const disabledMinutes = disabledTimeConfig.nzDisabledMinutes(hour);
            if (disabledMinutes.indexOf(minutes) === -1) {
                /** @type {?} */
                const disabledSeconds = disabledTimeConfig.nzDisabledSeconds(hour, minutes);
                invalidTime = disabledSeconds.indexOf(seconds) !== -1;
            }
            else {
                invalidTime = true;
            }
        }
        else {
            invalidTime = true;
        }
    }
    return !invalidTime;
}
/**
 * @param {?} value
 * @param {?} disabledTime
 * @return {?}
 */
export function isTimeValid(value, disabledTime) {
    /** @type {?} */
    const disabledTimeConfig = getTimeConfig(value, disabledTime);
    return isTimeValidByConfig(value, disabledTimeConfig);
}
/**
 * @param {?} value
 * @param {?=} disabledDate
 * @param {?=} disabledTime
 * @return {?}
 */
export function isAllowedDate(value, disabledDate, disabledTime) {
    if (disabledDate) {
        if (disabledDate(value.nativeDate)) {
            return false;
        }
    }
    if (disabledTime) {
        if (!isTimeValid(value, disabledTime)) {
            return false;
        }
    }
    return true;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9saWIvdXRpbC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztNQUdNLG1CQUFtQixHQUF1Qjs7OztJQUM5QyxlQUFlO1FBQ2IsT0FBTyxFQUFFLENBQUM7SUFDWixDQUFDOzs7O0lBQ0QsaUJBQWlCO1FBQ2YsT0FBTyxFQUFFLENBQUM7SUFDWixDQUFDOzs7O0lBQ0QsaUJBQWlCO1FBQ2YsT0FBTyxFQUFFLENBQUM7SUFDWixDQUFDO0NBQ0Y7Ozs7OztBQUVELE1BQU0sVUFBVSxhQUFhLENBQUMsS0FBZ0IsRUFBRSxZQUE0Qjs7UUFDdEUsa0JBQWtCLEdBQUcsWUFBWSxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsS0FBSyxJQUFJLEtBQUssQ0FBQyxVQUFVLENBQUMsQ0FBQyxDQUFDLENBQUMsbUJBQUEsRUFBRSxFQUFzQjtJQUMxRyxrQkFBa0IscUJBQ2IsbUJBQW1CLEVBQ25CLGtCQUFrQixDQUN0QixDQUFDO0lBQ0YsT0FBTyxrQkFBa0IsQ0FBQztBQUM1QixDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsbUJBQW1CLENBQUMsS0FBZ0IsRUFBRSxrQkFBc0M7O1FBQ3RGLFdBQVcsR0FBRyxLQUFLO0lBQ3ZCLElBQUksS0FBSyxFQUFFOztjQUNILElBQUksR0FBRyxLQUFLLENBQUMsUUFBUSxFQUFFOztjQUN2QixPQUFPLEdBQUcsS0FBSyxDQUFDLFVBQVUsRUFBRTs7Y0FDNUIsT0FBTyxHQUFHLEtBQUssQ0FBQyxVQUFVLEVBQUU7O2NBQzVCLGFBQWEsR0FBRyxrQkFBa0IsQ0FBQyxlQUFlLEVBQUU7UUFDMUQsSUFBSSxhQUFhLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFOztrQkFDaEMsZUFBZSxHQUFHLGtCQUFrQixDQUFDLGlCQUFpQixDQUFDLElBQUksQ0FBQztZQUNsRSxJQUFJLGVBQWUsQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUU7O3NCQUNyQyxlQUFlLEdBQUcsa0JBQWtCLENBQUMsaUJBQWlCLENBQUMsSUFBSSxFQUFFLE9BQU8sQ0FBQztnQkFDM0UsV0FBVyxHQUFHLGVBQWUsQ0FBQyxPQUFPLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUM7YUFDdkQ7aUJBQU07Z0JBQ0wsV0FBVyxHQUFHLElBQUksQ0FBQzthQUNwQjtTQUNGO2FBQU07WUFDTCxXQUFXLEdBQUcsSUFBSSxDQUFDO1NBQ3BCO0tBQ0Y7SUFDRCxPQUFPLENBQUMsV0FBVyxDQUFDO0FBQ3RCLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxXQUFXLENBQUMsS0FBZ0IsRUFBRSxZQUE0Qjs7VUFDbEUsa0JBQWtCLEdBQUcsYUFBYSxDQUFDLEtBQUssRUFBRSxZQUFZLENBQUM7SUFDN0QsT0FBTyxtQkFBbUIsQ0FBQyxLQUFLLEVBQUUsa0JBQWtCLENBQUMsQ0FBQztBQUN4RCxDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGFBQWEsQ0FBQyxLQUFnQixFQUFFLFlBQTZCLEVBQUUsWUFBNkI7SUFDMUcsSUFBSSxZQUFZLEVBQUU7UUFDaEIsSUFBSSxZQUFZLENBQUMsS0FBSyxDQUFDLFVBQVUsQ0FBQyxFQUFFO1lBQ2xDLE9BQU8sS0FBSyxDQUFDO1NBQ2Q7S0FDRjtJQUNELElBQUksWUFBWSxFQUFFO1FBQ2hCLElBQUksQ0FBQyxXQUFXLENBQUMsS0FBSyxFQUFFLFlBQVksQ0FBQyxFQUFFO1lBQ3JDLE9BQU8sS0FBSyxDQUFDO1NBQ2Q7S0FDRjtJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IERpc2FibGVkRGF0ZUZuLCBEaXNhYmxlZFRpbWVDb25maWcsIERpc2FibGVkVGltZUZuIH0gZnJvbSAnLi4vc3RhbmRhcmQtdHlwZXMnO1xuaW1wb3J0IHsgQ2FuZHlEYXRlIH0gZnJvbSAnLi9jYW5keS1kYXRlJztcblxuY29uc3QgZGVmYXVsdERpc2FibGVkVGltZTogRGlzYWJsZWRUaW1lQ29uZmlnID0ge1xuICBuekRpc2FibGVkSG91cnMoKTogbnVtYmVyW10ge1xuICAgIHJldHVybiBbXTtcbiAgfSxcbiAgbnpEaXNhYmxlZE1pbnV0ZXMoKTogbnVtYmVyW10ge1xuICAgIHJldHVybiBbXTtcbiAgfSxcbiAgbnpEaXNhYmxlZFNlY29uZHMoKTogbnVtYmVyW10ge1xuICAgIHJldHVybiBbXTtcbiAgfVxufTtcblxuZXhwb3J0IGZ1bmN0aW9uIGdldFRpbWVDb25maWcodmFsdWU6IENhbmR5RGF0ZSwgZGlzYWJsZWRUaW1lOiBEaXNhYmxlZFRpbWVGbik6IERpc2FibGVkVGltZUNvbmZpZyB7XG4gIGxldCBkaXNhYmxlZFRpbWVDb25maWcgPSBkaXNhYmxlZFRpbWUgPyBkaXNhYmxlZFRpbWUodmFsdWUgJiYgdmFsdWUubmF0aXZlRGF0ZSkgOiB7fSBhcyBEaXNhYmxlZFRpbWVDb25maWc7XG4gIGRpc2FibGVkVGltZUNvbmZpZyA9IHtcbiAgICAuLi5kZWZhdWx0RGlzYWJsZWRUaW1lLFxuICAgIC4uLmRpc2FibGVkVGltZUNvbmZpZ1xuICB9O1xuICByZXR1cm4gZGlzYWJsZWRUaW1lQ29uZmlnO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNUaW1lVmFsaWRCeUNvbmZpZyh2YWx1ZTogQ2FuZHlEYXRlLCBkaXNhYmxlZFRpbWVDb25maWc6IERpc2FibGVkVGltZUNvbmZpZyk6IGJvb2xlYW4ge1xuICBsZXQgaW52YWxpZFRpbWUgPSBmYWxzZTtcbiAgaWYgKHZhbHVlKSB7XG4gICAgY29uc3QgaG91ciA9IHZhbHVlLmdldEhvdXJzKCk7XG4gICAgY29uc3QgbWludXRlcyA9IHZhbHVlLmdldE1pbnV0ZXMoKTtcbiAgICBjb25zdCBzZWNvbmRzID0gdmFsdWUuZ2V0U2Vjb25kcygpO1xuICAgIGNvbnN0IGRpc2FibGVkSG91cnMgPSBkaXNhYmxlZFRpbWVDb25maWcubnpEaXNhYmxlZEhvdXJzKCk7XG4gICAgaWYgKGRpc2FibGVkSG91cnMuaW5kZXhPZihob3VyKSA9PT0gLTEpIHtcbiAgICAgIGNvbnN0IGRpc2FibGVkTWludXRlcyA9IGRpc2FibGVkVGltZUNvbmZpZy5uekRpc2FibGVkTWludXRlcyhob3VyKTtcbiAgICAgIGlmIChkaXNhYmxlZE1pbnV0ZXMuaW5kZXhPZihtaW51dGVzKSA9PT0gLTEpIHtcbiAgICAgICAgY29uc3QgZGlzYWJsZWRTZWNvbmRzID0gZGlzYWJsZWRUaW1lQ29uZmlnLm56RGlzYWJsZWRTZWNvbmRzKGhvdXIsIG1pbnV0ZXMpO1xuICAgICAgICBpbnZhbGlkVGltZSA9IGRpc2FibGVkU2Vjb25kcy5pbmRleE9mKHNlY29uZHMpICE9PSAtMTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGludmFsaWRUaW1lID0gdHJ1ZTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaW52YWxpZFRpbWUgPSB0cnVlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gIWludmFsaWRUaW1lO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNUaW1lVmFsaWQodmFsdWU6IENhbmR5RGF0ZSwgZGlzYWJsZWRUaW1lOiBEaXNhYmxlZFRpbWVGbik6IGJvb2xlYW4ge1xuICBjb25zdCBkaXNhYmxlZFRpbWVDb25maWcgPSBnZXRUaW1lQ29uZmlnKHZhbHVlLCBkaXNhYmxlZFRpbWUpO1xuICByZXR1cm4gaXNUaW1lVmFsaWRCeUNvbmZpZyh2YWx1ZSwgZGlzYWJsZWRUaW1lQ29uZmlnKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzQWxsb3dlZERhdGUodmFsdWU6IENhbmR5RGF0ZSwgZGlzYWJsZWREYXRlPzogRGlzYWJsZWREYXRlRm4sIGRpc2FibGVkVGltZT86IERpc2FibGVkVGltZUZuKTogYm9vbGVhbiB7XG4gIGlmIChkaXNhYmxlZERhdGUpIHtcbiAgICBpZiAoZGlzYWJsZWREYXRlKHZhbHVlLm5hdGl2ZURhdGUpKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG4gIGlmIChkaXNhYmxlZFRpbWUpIHtcbiAgICBpZiAoIWlzVGltZVZhbGlkKHZhbHVlLCBkaXNhYmxlZFRpbWUpKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG4gIHJldHVybiB0cnVlO1xufVxuIl19