/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { CandyDate } from '../candy-date';
import { getTimeConfig, isAllowedDate } from '../util';
var DateRangePopupComponent = /** @class */ (function () {
    function DateRangePopupComponent() {
        var _this = this;
        this.panelModeChange = new EventEmitter();
        this.valueChange = new EventEmitter();
        this.resultOk = new EventEmitter(); // Emitted when done with date selecting
        // Emitted when done with date selecting
        this.closePicker = new EventEmitter(); // Notify outside to close the picker panel
        // Notify outside to close the picker panel
        this.prefixCls = 'ant-calendar';
        this.showTimePicker = false;
        this.partTypeMap = { 'left': 0, 'right': 1 };
        this.disabledStartTime = (/**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            return _this.disabledTime && _this.disabledTime(value, 'start');
        });
        this.disabledEndTime = (/**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            return _this.disabledTime && _this.disabledTime(value, 'end');
        });
    }
    Object.defineProperty(DateRangePopupComponent.prototype, "hasTimePicker", {
        get: 
        // Range ONLY
        /**
         * @return {?}
         */
        function () {
            return !!this.showTime;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(DateRangePopupComponent.prototype, "hasFooter", {
        get: /**
         * @return {?}
         */
        function () {
            return this.showToday || this.hasTimePicker || !!this.extraFooter || !!this.ranges;
        },
        enumerable: true,
        configurable: true
    });
    // tslint:disable-line:no-any
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.ngOnInit = 
    // tslint:disable-line:no-any
    /**
     * @return {?}
     */
    function () {
        var _this = this;
        // Initialization for range properties to prevent errors while later assignment
        if (this.isRange) {
            ['placeholder', 'panelMode', 'selectedValue', 'hoverValue'].forEach((/**
             * @param {?} prop
             * @return {?}
             */
            function (prop) { return _this.initialArray(prop); }));
        }
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    DateRangePopupComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (this.isRange) {
            if (changes.value) { // Re-initialize all related values
                this.clearHoverValue();
                this.selectedValue = (/** @type {?} */ (this.value));
                this.valueForRangeShow = this.normalizeRangeValue((/** @type {?} */ (this.value)));
            }
        }
        // Parse showTime options
        if (changes.showTime || changes.disabledTime) {
            if (this.showTime) {
                this.buildTimeOptions();
            }
        }
        // Show time picker when assigned panel mode as "time"
        if (changes.panelMode && this.hasTimePicker) {
            this.showTimePicker = this.panelMode === 'time';
        }
    };
    /**
     * @param {?} show
     * @return {?}
     */
    DateRangePopupComponent.prototype.onShowTimePickerChange = /**
     * @param {?} show
     * @return {?}
     */
    function (show) {
        // this.panelMode = show ? 'time' : 'date';
        // this.panelModeChange.emit(this.panelMode);
        this.panelModeChange.emit(show ? 'time' : 'date');
    };
    /**
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.onClickToday = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        // if (this.isRange) { // Show today is not support by range
        //   throw new Error('"nzShowToday" is not support for "RangePicker"!');
        // } else {
        if (!this.isRange) {
            this.value = null; // Clear current value to not sync time by next step
            this.changeValue(value);
        }
        this.closePickerPanel();
    };
    /**
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.onDayHover = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.isRange && this.selectedValue[0] && !this.selectedValue[1]) { // When right value is selected, don't do hover
            // When right value is selected, don't do hover
            /** @type {?} */
            var base = this.selectedValue[0];
            if (base.isBefore(value, 'day')) {
                this.hoverValue = [base, value];
            }
            else {
                this.hoverValue = [value, base];
            }
        }
    };
    /**
     * @param {?} mode
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.onPanelModeChange = /**
     * @param {?} mode
     * @param {?=} partType
     * @return {?}
     */
    function (mode, partType) {
        if (this.isRange) {
            ((/** @type {?} */ (this.panelMode)))[this.getPartTypeIndex(partType)] = mode;
        }
        else {
            this.panelMode = mode;
        }
        this.panelModeChange.emit(this.panelMode);
    };
    /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.onHeaderChange = /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    function (value, partType) {
        if (this.isRange) {
            this.valueForRangeShow[this.getPartTypeIndex(partType)] = value;
            this.valueForRangeShow = this.normalizeRangeValue(this.valueForRangeShow); // Should always take care of start/end
        }
    };
    /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.onSelectTime = /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    function (value, partType) {
        if (this.isRange) {
            /** @type {?} */
            var newValue = this.cloneRangeDate((/** @type {?} */ (this.value)));
            /** @type {?} */
            var index = this.getPartTypeIndex(partType);
            newValue[index] = (/** @type {?} */ (this.overrideHms(value, newValue[index])));
            this.setValue(newValue);
        }
        else {
            this.setValue((/** @type {?} */ (this.overrideHms(value, ((/** @type {?} */ (this.value))) || new CandyDate())))); // If not select a date currently, use today
        }
    };
    /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.changeValue = /**
     * @param {?} value
     * @param {?=} partType
     * @return {?}
     */
    function (value, partType) {
        if (this.isRange) {
            /** @type {?} */
            var index = this.getPartTypeIndex(partType);
            this.selectedValue[index] = value;
            if (this.isValidRange(this.selectedValue)) {
                this.valueForRangeShow = this.normalizeRangeValue(this.selectedValue);
                this.setValue(this.cloneRangeDate(this.selectedValue));
            }
        }
        else {
            this.setValue(value);
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.changeValueFromSelect = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.isRange) {
            var _a = tslib_1.__read((/** @type {?} */ (this.selectedValue)), 2), left = _a[0], right = _a[1];
            if ((!left && !right) || (left && right)) { // If totally full or empty, clean up && re-assign left first
                this.hoverValue = this.selectedValue = [value];
            }
            else if (left && !right) { // If one of them is empty, assign the other one and sort, then set the final values
                this.clearHoverValue(); // Clean up
                this.setRangeValue('selectedValue', 'right', value);
                this.sortRangeValue('selectedValue'); // Sort
                this.valueForRangeShow = this.normalizeRangeValue(this.selectedValue);
                this.setValue(this.cloneRangeDate(this.selectedValue));
            }
        }
        else {
            this.setValue(value);
        }
        // this.selectDate.emit(value);
    };
    /**
     * @param {?} direction
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.enablePrevNext = /**
     * @param {?} direction
     * @param {?=} partType
     * @return {?}
     */
    function (direction, partType) {
        if (this.isRange) {
            var _a = tslib_1.__read(this.valueForRangeShow, 2), start = _a[0], end = _a[1];
            /** @type {?} */
            var showMiddle = !start.addMonths(1).isSame(end, 'month');
            if ((partType === 'left' && direction === 'next') || (partType === 'right' && direction === 'prev')) {
                return showMiddle;
            }
            return true;
        }
        else {
            return true;
        }
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getPanelMode = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        if (this.isRange) {
            return (/** @type {?} */ (this.panelMode[this.getPartTypeIndex(partType)]));
        }
        else {
            return (/** @type {?} */ (this.panelMode));
        }
    };
    // Get single value or part value of a range
    // Get single value or part value of a range
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getValue = 
    // Get single value or part value of a range
    /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        if (this.isRange) {
            return ((/** @type {?} */ (this.value)))[this.getPartTypeIndex(partType)];
        }
        else {
            return (/** @type {?} */ (this.value));
        }
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getValueBySelector = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        if (this.isRange) {
            /** @type {?} */
            var valueShow = this.showTimePicker ? this.value : this.valueForRangeShow;
            return ((/** @type {?} */ (valueShow)))[this.getPartTypeIndex(partType)];
        }
        else {
            return (/** @type {?} */ (this.value));
        }
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getPartTypeIndex = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        return this.partTypeMap[(/** @type {?} */ (partType))];
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getPlaceholder = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        return this.isRange ? this.placeholder[this.getPartTypeIndex(partType)] : (/** @type {?} */ (this.placeholder));
    };
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.hasSelectedValue = /**
     * @return {?}
     */
    function () {
        return this.selectedValue && !!this.selectedValue[1] && !!this.selectedValue[0];
    };
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.isAllowedSelectedValue = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var selectedValue = this.selectedValue;
        if (selectedValue && selectedValue[0] && selectedValue[1]) {
            return isAllowedDate(selectedValue[0], this.disabledDate, this.disabledStartTime) &&
                isAllowedDate(selectedValue[1], this.disabledDate, this.disabledEndTime);
        }
        return false;
    };
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.timePickerDisabled = /**
     * @return {?}
     */
    function () {
        if (!this.hasTimePicker) {
            return true;
        }
        if (this.isRange) {
            return !this.hasSelectedValue() || !!this.hoverValue.length;
        }
        else {
            return false;
        }
    };
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.okDisabled = /**
     * @return {?}
     */
    function () {
        if (!this.hasTimePicker) {
            return true;
        }
        if (this.isRange) {
            return !this.isAllowedSelectedValue() || !this.hasSelectedValue() || !!this.hoverValue.length;
        }
        else {
            return this.value ? !isAllowedDate((/** @type {?} */ (this.value)), this.disabledDate, this.disabledTime) : false;
        }
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    DateRangePopupComponent.prototype.getTimeOptions = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        if (this.showTime && this.timeOptions) {
            return this.timeOptions instanceof Array
                ? this.timeOptions[this.getPartTypeIndex(partType)]
                : this.timeOptions;
        }
        return null;
    };
    /**
     * @param {?} val
     * @return {?}
     */
    DateRangePopupComponent.prototype.onClickPresetRange = /**
     * @param {?} val
     * @return {?}
     */
    function (val) {
        /** @type {?} */
        var value = val;
        this.setValue([new CandyDate(value[0]), new CandyDate(value[1])]);
        this.resultOk.emit();
    };
    /**
     * @return {?}
     */
    DateRangePopupComponent.prototype.onPresetRangeMouseLeave = /**
     * @return {?}
     */
    function () {
        this.clearHoverValue();
    };
    /**
     * @param {?} val
     * @return {?}
     */
    DateRangePopupComponent.prototype.onHoverPresetRange = /**
     * @param {?} val
     * @return {?}
     */
    function (val) {
        this.hoverValue = ([new CandyDate(val[0]), new CandyDate(val[1])]);
    };
    /**
     * @param {?} obj
     * @return {?}
     */
    DateRangePopupComponent.prototype.getObjectKeys = /**
     * @param {?} obj
     * @return {?}
     */
    function (obj) {
        return obj ? Object.keys(obj) : [];
    };
    /**
     * @private
     * @return {?}
     */
    DateRangePopupComponent.prototype.closePickerPanel = /**
     * @private
     * @return {?}
     */
    function () {
        this.closePicker.emit();
    };
    /**
     * @private
     * @return {?}
     */
    DateRangePopupComponent.prototype.clearHoverValue = /**
     * @private
     * @return {?}
     */
    function () {
        this.hoverValue = [];
    };
    /**
     * @private
     * @return {?}
     */
    DateRangePopupComponent.prototype.buildTimeOptions = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.showTime) {
            /** @type {?} */
            var showTime = typeof this.showTime === 'object' ? this.showTime : {};
            if (this.isRange) {
                /** @type {?} */
                var value = (/** @type {?} */ (this.value));
                this.timeOptions = [
                    this.overrideTimeOptions(showTime, value[0], 'start'),
                    this.overrideTimeOptions(showTime, value[1], 'end')
                ];
            }
            else {
                this.timeOptions = this.overrideTimeOptions(showTime, (/** @type {?} */ (this.value)));
            }
        }
        else {
            this.timeOptions = null;
        }
    };
    /**
     * @private
     * @param {?} origin
     * @param {?} value
     * @param {?=} partial
     * @return {?}
     */
    DateRangePopupComponent.prototype.overrideTimeOptions = /**
     * @private
     * @param {?} origin
     * @param {?} value
     * @param {?=} partial
     * @return {?}
     */
    function (origin, value, partial) {
        /** @type {?} */
        var disabledTimeFn;
        if (partial) {
            disabledTimeFn = partial === 'start' ? this.disabledStartTime : this.disabledEndTime;
        }
        else {
            disabledTimeFn = this.disabledTime;
        }
        return tslib_1.__assign({}, origin, getTimeConfig(value, disabledTimeFn));
    };
    // Set value and trigger change event
    // Set value and trigger change event
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.setValue = 
    // Set value and trigger change event
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        /** @type {?} */
        var newValue = value;
        // TODO: Sync original time (NOTE: this should take more care of beacuse it may depend on many change sources)
        // if (this.isRange) {
        //   // TODO: Sync time
        // } else {
        //   if (this.value) { // Sync time from the original one if it's available
        //     newValue = this.overrideHms(this.value as CandyDate, newValue as CandyDate);
        //   }
        // }
        this.value = newValue;
        this.valueChange.emit(this.value);
        this.buildTimeOptions();
    };
    /**
     * @private
     * @param {?} from
     * @param {?} to
     * @return {?}
     */
    DateRangePopupComponent.prototype.overrideHms = /**
     * @private
     * @param {?} from
     * @param {?} to
     * @return {?}
     */
    function (from, to) {
        if (!from || !to) {
            return null;
        }
        return to.setHms(from.getHours(), from.getMinutes(), from.getSeconds());
    };
    // Check if it's a valid range value
    // Check if it's a valid range value
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.isValidRange = 
    // Check if it's a valid range value
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (Array.isArray(value)) {
            var _a = tslib_1.__read(value, 2), start = _a[0], end = _a[1];
            /** @type {?} */
            var grain = this.hasTimePicker ? 'second' : 'day';
            return start && end && (start.isBefore(end, grain) || start.isSame(end, grain));
        }
        return false;
    };
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.normalizeRangeValue = /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        var _a = tslib_1.__read(value, 2), start = _a[0], end = _a[1];
        /** @type {?} */
        var newStart = start || new CandyDate();
        /** @type {?} */
        var newEnd = end && end.isSame(newStart, 'month') ? end.addMonths(1) : end || newStart.addMonths(1);
        return [newStart, newEnd];
    };
    // private isEmptyRangeValue(value: CandyDate[]): boolean {
    //   return !value || !Array.isArray(value) || value.every((val) => !val);
    // }
    // Sort a range value (accurate to second)
    // private isEmptyRangeValue(value: CandyDate[]): boolean {
    //   return !value || !Array.isArray(value) || value.every((val) => !val);
    // }
    // Sort a range value (accurate to second)
    /**
     * @private
     * @param {?} key
     * @return {?}
     */
    DateRangePopupComponent.prototype.sortRangeValue = 
    // private isEmptyRangeValue(value: CandyDate[]): boolean {
    //   return !value || !Array.isArray(value) || value.every((val) => !val);
    // }
    // Sort a range value (accurate to second)
    /**
     * @private
     * @param {?} key
     * @return {?}
     */
    function (key) {
        if (Array.isArray(this[key])) {
            var _a = tslib_1.__read(this[key], 2), start = _a[0], end = _a[1];
            if (start && end && start.isAfter(end, 'day')) {
                this[key] = [end, start];
            }
        }
    };
    // Renew and set a range value to trigger sub-component's change detection
    // Renew and set a range value to trigger sub-component's change detection
    /**
     * @private
     * @param {?} key
     * @param {?} partType
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.setRangeValue = 
    // Renew and set a range value to trigger sub-component's change detection
    /**
     * @private
     * @param {?} key
     * @param {?} partType
     * @param {?} value
     * @return {?}
     */
    function (key, partType, value) {
        /** @type {?} */
        var ref = this[key] = this.cloneRangeDate((/** @type {?} */ (this[key])));
        ref[this.getPartTypeIndex(partType)] = value;
    };
    /**
     * @private
     * @param {?} value
     * @return {?}
     */
    DateRangePopupComponent.prototype.cloneRangeDate = /**
     * @private
     * @param {?} value
     * @return {?}
     */
    function (value) {
        return (/** @type {?} */ ([value[0] && value[0].clone(), value[1] && value[1].clone()]));
    };
    /**
     * @private
     * @param {?} key
     * @return {?}
     */
    DateRangePopupComponent.prototype.initialArray = /**
     * @private
     * @param {?} key
     * @return {?}
     */
    function (key) {
        if (!this[key] || !Array.isArray(this[key])) {
            this[key] = [];
        }
    };
    DateRangePopupComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    // tslint:disable-next-line:component-selector
                    selector: 'date-range-popup',
                    template: "<div\n  class=\"{{ prefixCls }}-picker-container {{ dropdownClassName }} {{ prefixCls }}-picker-container-placement-bottomLeft\"\n  [ngStyle]=\"popupStyle\">\n\n  <div class=\"{{ prefixCls }} {{ showWeek ? prefixCls + '-week-number': '' }} {{ hasTimePicker ? prefixCls + '-time' : '' }} {{ isRange ? prefixCls + '-range' : '' }}\" tabindex=\"0\">\n    <div class=\"{{ prefixCls }}-panel\">\n      <ng-container *ngIf=\"!isRange\"> <!-- Single ONLY -->\n        <ng-container *ngTemplateOutlet=\"tplCalendarInput\"></ng-container>\n      </ng-container>\n      <div class=\"{{ prefixCls }}-date-panel\">\n        <ng-container *ngIf=\"isRange; else tplSinglePart\">\n          <!-- Range Selectors -->\n          <ng-container *ngTemplateOutlet=\"tplRangePart; context: { partType: 'left' }\"></ng-container>\n          <div class=\"ant-calendar-range-middle\">~</div>\n          <ng-container *ngTemplateOutlet=\"tplRangePart; context: { partType: 'right' }\"></ng-container>\n        </ng-container>\n\n        <ng-container *ngIf=\"!isRange\"> <!-- Single ONLY -->\n          <ng-container *ngTemplateOutlet=\"tplFooter\"></ng-container>\n        </ng-container>\n      </div>\n      <ng-container *ngIf=\"isRange\"> <!-- Range ONLY -->\n        <ng-container *ngTemplateOutlet=\"tplFooter\"></ng-container>\n      </ng-container>\n    </div>\n  </div>\n</div>\n\n<ng-template #tplCalendarInput let-partType=\"partType\">\n  <calendar-input\n    [value]=\"getValue(partType)\"\n    (valueChange)=\"changeValue($event, partType)\"\n    [locale]=\"locale\"\n    [disabledDate]=\"disabledDate\"\n    [format]=\"format\"\n    [placeholder]=\"getPlaceholder(partType)\"\n  ></calendar-input>\n</ng-template>\n\n<ng-template #tplInnerPopup let-partType=\"partType\">\n  <inner-popup\n    [showWeek]=\"showWeek\"\n    [locale]=\"locale\"\n    [showTimePicker]=\"hasTimePicker && showTimePicker\"\n    [timeOptions]=\"getTimeOptions(partType)\"\n    [panelMode]=\"getPanelMode(partType)\"\n    (panelModeChange)=\"onPanelModeChange($event, partType)\"\n    [value]=\"getValueBySelector(partType)\"\n    [disabledDate]=\"disabledDate\"\n    [dateRender]=\"dateRender\"\n    [selectedValue]=\"selectedValue\"\n    [hoverValue]=\"hoverValue\"\n    [enablePrev]=\"enablePrevNext('prev', partType)\"\n    [enableNext]=\"enablePrevNext('next', partType)\"\n    (dayHover)=\"onDayHover($event)\"\n    (selectDate)=\"changeValueFromSelect($event)\"\n    (selectTime)=\"onSelectTime($event, partType)\"\n    (headerChange)=\"onHeaderChange($event, partType)\"\n  ></inner-popup>\n</ng-template>\n\n<ng-template #tplFooter>\n  <calendar-footer\n    *ngIf=\"hasFooter\"\n    [locale]=\"locale\"\n    [showToday]=\"showToday\"\n    [hasTimePicker]=\"hasTimePicker\"\n    [timePickerDisabled]=\"timePickerDisabled()\"\n    [okDisabled]=\"okDisabled()\"\n    [extraFooter]=\"extraFooter\"\n    [rangeQuickSelector]=\"ranges ? tplRangeQuickSelector : null\"\n    [(showTimePicker)]=\"showTimePicker\"\n    (showTimePickerChange)=\"onShowTimePickerChange($event)\"\n    (clickOk)=\"resultOk.emit()\"\n    (clickToday)=\"onClickToday($event)\"\n  ></calendar-footer>\n</ng-template>\n\n<!-- Single ONLY -->\n<ng-template #tplSinglePart>\n  <ng-container *ngTemplateOutlet=\"tplInnerPopup\"></ng-container>\n</ng-template>\n\n<!-- Range ONLY -->\n<ng-template #tplRangePart let-partType=\"partType\">\n  <div class=\"{{ prefixCls }}-range-part {{ prefixCls }}-range-{{ partType }}\">\n    <ng-container *ngTemplateOutlet=\"tplCalendarInput; context: { partType: partType }\"></ng-container>\n    <div style=\"outline: none;\">\n      <ng-container *ngTemplateOutlet=\"tplInnerPopup; context: { partType: partType }\"></ng-container>\n    </div>\n  </div>\n</ng-template>\n\n<!-- Range ONLY: Range Quick Selector -->\n<ng-template #tplRangeQuickSelector>\n  <a *ngFor=\"let name of getObjectKeys(ranges)\"\n    (click)=\"onClickPresetRange(ranges[name])\"\n    (mouseenter)=\"onHoverPresetRange(ranges[name])\"\n    (mouseleave)=\"onPresetRangeMouseLeave()\"\n  >{{ name }}</a>\n</ng-template>"
                }] }
    ];
    DateRangePopupComponent.propDecorators = {
        isRange: [{ type: Input }],
        showWeek: [{ type: Input }],
        locale: [{ type: Input }],
        format: [{ type: Input }],
        placeholder: [{ type: Input }],
        disabledDate: [{ type: Input }],
        disabledTime: [{ type: Input }],
        showToday: [{ type: Input }],
        showTime: [{ type: Input }],
        extraFooter: [{ type: Input }],
        ranges: [{ type: Input }],
        dateRender: [{ type: Input }],
        popupStyle: [{ type: Input }],
        dropdownClassName: [{ type: Input }],
        panelMode: [{ type: Input }],
        panelModeChange: [{ type: Output }],
        value: [{ type: Input }],
        valueChange: [{ type: Output }],
        resultOk: [{ type: Output }],
        closePicker: [{ type: Output }]
    };
    return DateRangePopupComponent;
}());
export { DateRangePopupComponent };
if (false) {
    /** @type {?} */
    DateRangePopupComponent.prototype.isRange;
    /** @type {?} */
    DateRangePopupComponent.prototype.showWeek;
    /** @type {?} */
    DateRangePopupComponent.prototype.locale;
    /** @type {?} */
    DateRangePopupComponent.prototype.format;
    /** @type {?} */
    DateRangePopupComponent.prototype.placeholder;
    /** @type {?} */
    DateRangePopupComponent.prototype.disabledDate;
    /** @type {?} */
    DateRangePopupComponent.prototype.disabledTime;
    /** @type {?} */
    DateRangePopupComponent.prototype.showToday;
    /** @type {?} */
    DateRangePopupComponent.prototype.showTime;
    /** @type {?} */
    DateRangePopupComponent.prototype.extraFooter;
    /** @type {?} */
    DateRangePopupComponent.prototype.ranges;
    /** @type {?} */
    DateRangePopupComponent.prototype.dateRender;
    /** @type {?} */
    DateRangePopupComponent.prototype.popupStyle;
    /** @type {?} */
    DateRangePopupComponent.prototype.dropdownClassName;
    /** @type {?} */
    DateRangePopupComponent.prototype.panelMode;
    /** @type {?} */
    DateRangePopupComponent.prototype.panelModeChange;
    /** @type {?} */
    DateRangePopupComponent.prototype.value;
    /** @type {?} */
    DateRangePopupComponent.prototype.valueChange;
    /** @type {?} */
    DateRangePopupComponent.prototype.resultOk;
    /** @type {?} */
    DateRangePopupComponent.prototype.closePicker;
    /** @type {?} */
    DateRangePopupComponent.prototype.prefixCls;
    /** @type {?} */
    DateRangePopupComponent.prototype.showTimePicker;
    /** @type {?} */
    DateRangePopupComponent.prototype.timeOptions;
    /** @type {?} */
    DateRangePopupComponent.prototype.valueForRangeShow;
    /** @type {?} */
    DateRangePopupComponent.prototype.selectedValue;
    /** @type {?} */
    DateRangePopupComponent.prototype.hoverValue;
    /**
     * @type {?}
     * @private
     */
    DateRangePopupComponent.prototype.partTypeMap;
    /** @type {?} */
    DateRangePopupComponent.prototype.disabledStartTime;
    /** @type {?} */
    DateRangePopupComponent.prototype.disabledEndTime;
    /* Skipping unhandled member: [ property: string ]: any*/
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZS1yYW5nZS1wb3B1cC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZGF0ZS1waWNrZXIvbGliL3BvcHVwcy9kYXRlLXJhbmdlLXBvcHVwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFlBQVksRUFDWixLQUFLLEVBR0wsTUFBTSxFQUdOLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQWF2QixPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQzFDLE9BQU8sRUFBRSxhQUFhLEVBQUUsYUFBYSxFQUFFLE1BQU0sU0FBUyxDQUFDO0FBRXZEO0lBQUE7UUFBQSxpQkE0WUM7UUFsWG9CLG9CQUFlLEdBQUcsSUFBSSxZQUFZLEVBQTJCLENBQUM7UUFHOUQsZ0JBQVcsR0FBRyxJQUFJLFlBQVksRUFBMkIsQ0FBQztRQUUxRCxhQUFRLEdBQUcsSUFBSSxZQUFZLEVBQVEsQ0FBQyxDQUFDLHdDQUF3Qzs7UUFDN0UsZ0JBQVcsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDLENBQUMsMkNBQTJDOztRQUV0RyxjQUFTLEdBQVcsY0FBYyxDQUFDO1FBQ25DLG1CQUFjLEdBQVksS0FBSyxDQUFDO1FBY3hCLGdCQUFXLEdBQWdDLEVBQUUsTUFBTSxFQUFFLENBQUMsRUFBRSxPQUFPLEVBQUUsQ0FBQyxFQUFFLENBQUM7UUE0SzdFLHNCQUFpQjs7OztRQUFHLFVBQUMsS0FBb0I7WUFDdkMsT0FBTyxLQUFJLENBQUMsWUFBWSxJQUFJLEtBQUksQ0FBQyxZQUFZLENBQUMsS0FBSyxFQUFFLE9BQU8sQ0FBQyxDQUFDO1FBQ2hFLENBQUMsRUFBQTtRQUVELG9CQUFlOzs7O1FBQUcsVUFBQyxLQUFvQjtZQUNyQyxPQUFPLEtBQUksQ0FBQyxZQUFZLElBQUksS0FBSSxDQUFDLFlBQVksQ0FBQyxLQUFL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QUE4RDs7SUFDOUQsb0RBQStCOztJQUMvQixnREFBMkI7O0lBQzNCLDZDQUF3Qjs7Ozs7SUFVeEIsOENBQTZFOztJQTRLN0Usb0RBRUM7O0lBRUQsa0RBRUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IEZ1bmN0aW9uUHJvcCB9IGZyb20gJy4uLy4uLy4uL2NvcmUvdHlwZXMvY29tbW9uLXdyYXAnO1xuaW1wb3J0IHsgTnpDYWxlbmRhckkxOG5JbnRlcmZhY2UgfSBmcm9tICcuLi8uLi8uLi9pMThuL256LWkxOG4uaW50ZXJmYWNlJztcbmltcG9ydCB7XG4gIERpc2FibGVkRGF0ZUZuLFxuICBEaXNhYmxlZFRpbWVDb25maWcsXG4gIERpc2FibGVkVGltZUZuLFxuICBEaXNhYmxlZFRpbWVQYXJ0aWFsLFxuICBQYW5lbE1vZGUsXG4gIFByZXNldFJhbmdlcyxcbiAgU3VwcG9ydFRpbWVPcHRpb25zXG59IGZyb20gJy4uLy4uL3N0YW5kYXJkLXR5cGVzJztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4uL2NhbmR5LWRhdGUnO1xuaW1wb3J0IHsgZ2V0VGltZUNvbmZpZywgaXNBbGxvd2VkRGF0ZSB9IGZyb20gJy4uL3V0aWwnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpjb21wb25lbnQtc2VsZWN0b3JcbiAgc2VsZWN0b3I6ICdkYXRlLXJhbmdlLXBvcHVwJyxcbiAgdGVtcGxhdGVVcmw6ICdkYXRlLXJhbmdlLXBvcHVwLmNvbXBvbmVudC5odG1sJ1xufSlcblxuZXhwb3J0IGNsYXNzIERhdGVSYW5nZVBvcHVwQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBpc1JhbmdlOiBib29sZWFuO1xuICBASW5wdXQoKSBzaG93V2VlazogYm9vbGVhbjtcblxuICBASW5wdXQoKSBsb2NhbGU6IE56Q2FsZW5kYXJJMThuSW50ZXJmYWNlO1xuICBASW5wdXQoKSBmb3JtYXQ6IHN0cmluZztcbiAgQElucHV0KCkgcGxhY2Vob2xkZXI6IHN0cmluZyB8IHN0cmluZ1tdO1xuICBASW5wdXQoKSBkaXNhYmxlZERhdGU6IERpc2FibGVkRGF0ZUZuO1xuICBASW5wdXQoKSBkaXNhYmxlZFRpbWU6IERpc2FibGVkVGltZUZuOyAvLyBUaGlzIHdpbGwgbGVhZCB0byByZWJ1aWxkIHRpbWUgb3B0aW9uc1xuICBASW5wdXQoKSBzaG93VG9kYXk6IGJvb2xlYW47XG4gIEBJbnB1dCgpIHNob3dUaW1lOiBTdXBwb3J0VGltZU9wdGlvbnMgfCBib29sZWFuO1xuICBASW5wdXQoKSBleHRyYUZvb3RlcjogVGVtcGxhdGVSZWY8dm9pZD4gfCBzdHJpbmc7XG4gIEBJbnB1dCgpIHJhbmdlczogUHJlc2V0UmFuZ2VzO1xuICBASW5wdXQoKSBkYXRlUmVuZGVyOiBGdW5jdGlvblByb3A8VGVtcGxhdGVSZWY8RGF0ZT4gfCBzdHJpbmc+O1xuICBASW5wdXQoKSBwb3B1cFN0eWxlOiBvYmplY3Q7XG4gIEBJbnB1dCgpIGRyb3Bkb3duQ2xhc3NOYW1lOiBzdHJpbmc7XG5cbiAgQElucHV0KCkgcGFuZWxNb2RlOiBQYW5lbE1vZGUgfCBQYW5lbE1vZGVbXTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IHBhbmVsTW9kZUNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8UGFuZWxNb2RlIHwgUGFuZWxNb2RlW10+KCk7XG5cbiAgQElucHV0KCkgdmFsdWU6IENhbmR5RGF0ZSB8IENhbmR5RGF0ZVtdIHwgbnVsbDtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IHZhbHVlQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxDYW5keURhdGUgfCBDYW5keURhdGVbXT4oKTtcblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgcmVzdWx0T2sgPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7IC8vIEVtaXR0ZWQgd2hlbiBkb25lIHdpdGggZGF0ZSBzZWxlY3RpbmdcbiAgQE91dHB1dCgpIHJlYWRvbmx5IGNsb3NlUGlja2VyID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpOyAvLyBOb3RpZnkgb3V0c2lkZSB0byBjbG9zZSB0aGUgcGlja2VyIHBhbmVsXG5cbiAgcHJlZml4Q2xzOiBzdHJpbmcgPSAnYW50LWNhbGVuZGFyJztcbiAgc2hvd1RpbWVQaWNrZXI6IGJvb2xlYW4gPSBmYWxzZTtcbiAgdGltZU9wdGlvbnM6IFN1cHBvcnRUaW1lT3B0aW9ucyB8IFN1cHBvcnRUaW1lT3B0aW9uc1tdIHwgbnVsbDtcbiAgdmFsdWVGb3JSYW5nZVNob3c6IENhbmR5RGF0ZVtdOyAvLyBSYW5nZSBPTkxZXG4gIHNlbGVjdGVkVmFsdWU6IENhbmR5RGF0ZVtdOyAvLyBSYW5nZSBPTkxZXG4gIGhvdmVyVmFsdWU6IENhbmR5RGF0ZVtdOyAvLyBSYW5nZSBPTkxZXG5cbiAgZ2V0IGhhc1RpbWVQaWNrZXIoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuICEhdGhpcy5zaG93VGltZTtcbiAgfVxuXG4gIGdldCBoYXNGb290ZXIoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuc2hvd1RvZGF5IHx8IHRoaXMuaGFzVGltZVBpY2tlciB8fCAhIXRoaXMuZXh0cmFGb290ZXIgfHwgISF0aGlzLnJhbmdlcztcbiAgfVxuXG4gIHByaXZhdGUgcGFydFR5cGVNYXA6IHsgWyBrZXk6IHN0cmluZyBdOiBudW1iZXIgfSA9IHsgJ2xlZnQnOiAwLCAncmlnaHQnOiAxIH07XG5cbiAgWyBwcm9wZXJ0eTogc3RyaW5nIF06IGFueSAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIC8vIEluaXRpYWxpemF0aW9uIGZvciByYW5nZSBwcm9wZXJ0aWVzIHRvIHByZXZlbnQgZXJyb3JzIHdoaWxlIGxhdGVyIGFzc2lnbm1lbnRcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICBbICdwbGFjZWhvbGRlcicsICdwYW5lbE1vZGUnLCAnc2VsZWN0ZWRWYWx1ZScsICdob3ZlclZhbHVlJyBdLmZvckVhY2goKHByb3ApID0+IHRoaXMuaW5pdGlhbEFycmF5KHByb3ApKTtcbiAgICB9XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgaWYgKGNoYW5nZXMudmFsdWUpIHsgLy8gUmUtaW5pdGlhbGl6ZSBhbGwgcmVsYXRlZCB2YWx1ZXNcbiAgICAgICAgdGhpcy5jbGVhckhvdmVyVmFsdWUoKTtcbiAgICAgICAgdGhpcy5zZWxlY3RlZFZhbHVlID0gdGhpcy52YWx1ZSBhcyBDYW5keURhdGVbXTtcbiAgICAgICAgdGhpcy52YWx1ZUZvclJhbmdlU2hvdyA9IHRoaXMubm9ybWFsaXplUmFuZ2VWYWx1ZSh0aGlzLnZhbHVlIGFzIENhbmR5RGF0ZVtdKTtcbiAgICAgIH1cbiAgICB9XG5cbiAgICAvLyBQYXJzZSBzaG93VGltZSBvcHRpb25zXG4gICAgaWYgKGNoYW5nZXMuc2hvd1RpbWUgfHwgY2hhbmdlcy5kaXNhYmxlZFRpbWUpIHtcbiAgICAgIGlmICh0aGlzLnNob3dUaW1lKSB7XG4gICAgICAgIHRoaXMuYnVpbGRUaW1lT3B0aW9ucygpO1xuICAgICAgfVxuICAgIH1cblxuICAgIC8vIFNob3cgdGltZSBwaWNrZXIgd2hlbiBhc3NpZ25lZCBwYW5lbCBtb2RlIGFzIFwidGltZVwiXG4gICAgaWYgKGNoYW5nZXMucGFuZWxNb2RlICYmIHRoaXMuaGFzVGltZVBpY2tlcikge1xuICAgICAgdGhpcy5zaG93VGltZVBpY2tlciA9IHRoaXMucGFuZWxNb2RlID09PSAndGltZSc7XG4gICAgfVxuICB9XG5cbiAgb25TaG93VGltZVBpY2tlckNoYW5nZShzaG93OiBib29sZWFuKTogdm9pZCB7XG4gICAgLy8gdGhpcy5wYW5lbE1vZGUgPSBzaG93ID8gJ3RpbWUnIDogJ2RhdGUnO1xuICAgIC8vIHRoaXMucGFuZWxNb2RlQ2hhbmdlLmVtaXQodGhpcy5wYW5lbE1vZGUpO1xuICAgIHRoaXMucGFuZWxNb2RlQ2hhbmdlLmVtaXQoc2hvdyA/ICd0aW1lJyA6ICdkYXRlJyk7XG4gIH1cblxuICBvbkNsaWNrVG9kYXkodmFsdWU6IENhbmR5RGF0ZSk6IHZvaWQge1xuICAgIC8vIGlmICh0aGlzLmlzUmFuZ2UpIHsgLy8gU2hvdyB0b2RheSBpcyBub3Qgc3VwcG9ydCBieSByYW5nZVxuICAgIC8vICAgdGhyb3cgbmV3IEVycm9yKCdcIm56U2hvd1RvZGF5XCIgaXMgbm90IHN1cHBvcnQgZm9yIFwiUmFuZ2VQaWNrZXJcIiEnKTtcbiAgICAvLyB9IGVsc2Uge1xuICAgIGlmICghdGhpcy5pc1JhbmdlKSB7XG4gICAgICB0aGlzLnZhbHVlID0gbnVsbDsgLy8gQ2xlYXIgY3VycmVudCB2YWx1ZSB0byBub3Qgc3luYyB0aW1lIGJ5IG5leHQgc3RlcFxuICAgICAgdGhpcy5jaGFuZ2VWYWx1ZSh2YWx1ZSk7XG4gICAgfVxuICAgIHRoaXMuY2xvc2VQaWNrZXJQYW5lbCgpO1xuICB9XG5cbiAgb25EYXlIb3Zlcih2YWx1ZTogQ2FuZHlEYXRlKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSAmJiB0aGlzLnNlbGVjdGVkVmFsdWVbIDAgXSAmJiAhdGhpcy5zZWxlY3RlZFZhbHVlWyAxIF0pIHsgLy8gV2hlbiByaWdodCB2YWx1ZSBpcyBzZWxlY3RlZCwgZG9uJ3QgZG8gaG92ZXJcbiAgICAgIGNvbnN0IGJhc2UgPSB0aGlzLnNlbGVjdGVkVmFsdWVbIDAgXTsgLy8gVXNlIHRoZSBsZWZ0IG9mIHNlbGVjdGVkIHZhbHVlIGFzIHRoZSBiYXNlIHRvIGRlY2lkZSBsYXRlciBob3ZlclZhbHVlXG4gICAgICBpZiAoYmFzZS5pc0JlZm9yZSh2YWx1ZSwgJ2RheScpKSB7XG4gICAgICAgIHRoaXMuaG92ZXJWYWx1ZSA9IFsgYmFzZSwgdmFsdWUgXTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuaG92ZXJWYWx1ZSA9IFsgdmFsdWUsIGJhc2UgXTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBvblBhbmVsTW9kZUNoYW5nZShtb2RlOiBQYW5lbE1vZGUsIHBhcnRUeXBlPzogUmFuZ2VQYXJ0VHlwZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgICh0aGlzLnBhbmVsTW9kZSBhcyBQYW5lbE1vZGVbXSlbIHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSkgXSA9IG1vZGU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucGFuZWxNb2RlID0gbW9kZTtcbiAgICB9XG4gICAgdGhpcy5wYW5lbE1vZGVDaGFuZ2UuZW1pdCh0aGlzLnBhbmVsTW9kZSk7XG4gIH1cblxuICBvbkhlYWRlckNoYW5nZSh2YWx1ZTogQ2FuZHlEYXRlLCBwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICB0aGlzLnZhbHVlRm9yUmFuZ2VTaG93WyB0aGlzLmdldFBhcnRUeXBlSW5kZXgocGFydFR5cGUpIF0gPSB2YWx1ZTtcbiAgICAgIHRoaXMudmFsdWVGb3JSYW5nZVNob3cgPSB0aGlzLm5vcm1hbGl6ZVJhbmdlVmFsdWUodGhpcy52YWx1ZUZvclJhbmdlU2hvdyk7IC8vIFNob3VsZCBhbHdheXMgdGFrZSBjYXJlIG9mIHN0YXJ0L2VuZFxuICAgIH1cbiAgfVxuXG4gIG9uU2VsZWN0VGltZSh2YWx1ZTogQ2FuZHlEYXRlLCBwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICBjb25zdCBuZXdWYWx1ZSA9IHRoaXMuY2xvbmVSYW5nZURhdGUodGhpcy52YWx1ZSBhcyBDYW5keURhdGVbXSk7XG4gICAgICBjb25zdCBpbmRleCA9IHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSk7XG4gICAgICBuZXdWYWx1ZVsgaW5kZXggXSA9IHRoaXMub3ZlcnJpZGVIbXModmFsdWUsIG5ld1ZhbHVlWyBpbmRleCBdKSE7XG4gICAgICB0aGlzLnNldFZhbHVlKG5ld1ZhbHVlKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5zZXRWYWx1ZSh0aGlzLm92ZXJyaWRlSG1zKHZhbHVlLCAodGhpcy52YWx1ZSBhcyBDYW5keURhdGUpIHx8IG5ldyBDYW5keURhdGUoKSkhKTsgLy8gSWYgbm90IHNlbGVjdCBhIGRhdGUgY3VycmVudGx5LCB1c2UgdG9kYXlcbiAgICB9XG4gIH1cblxuICBjaGFuZ2VWYWx1ZSh2YWx1ZTogQ2FuZHlEYXRlLCBwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICBjb25zdCBpbmRleCA9IHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSk7XG4gICAgICB0aGlzLnNlbGVjdGVkVmFsdWVbIGluZGV4IF0gPSB2YWx1ZTtcbiAgICAgIGlmICh0aGlzLmlzVmFsaWRSYW5nZSh0aGlzLnNlbGVjdGVkVmFsdWUpKSB7XG4gICAgICAgIHRoaXMudmFsdWVGb3JSYW5nZVNob3cgPSB0aGlzLm5vcm1hbGl6ZVJhbmdlVmFsdWUodGhpcy5zZWxlY3RlZFZhbHVlKTtcbiAgICAgICAgdGhpcy5zZXRWYWx1ZSh0aGlzLmNsb25lUmFuZ2VEYXRlKHRoaXMuc2VsZWN0ZWRWYWx1ZSkpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnNldFZhbHVlKHZhbHVlKTtcbiAgICB9XG4gIH1cblxuICBjaGFuZ2VWYWx1ZUZyb21TZWxlY3QodmFsdWU6IENhbmR5RGF0ZSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIGNvbnN0IFsgbGVmdCwgcmlnaHQgXSA9IHRoaXMuc2VsZWN0ZWRWYWx1ZSBhcyBDYW5keURhdGVbXTsgLy8gTk9URTogdGhlIGxlZnQvcmlnaHQgbWF5YmUgbm90IHRoZSBzZXF1ZW5jZSBpdCBzZWxlY3QgYXQgdGhlIGRhdGUgcGFuZWxzXG5cbiAgICAgIGlmICgoIWxlZnQgJiYgIXJpZ2h0KSB8fCAobGVmdCAmJiByaWdodCkpIHsgLy8gSWYgdG90YWxseSBmdWxsIG9yIGVtcHR5LCBjbGVhbiB1cCAmJiByZS1hc3NpZ24gbGVmdCBmaXJzdFxuICAgICAgICB0aGlzLmhvdmVyVmFsdWUgPSB0aGlzLnNlbGVjdGVkVmFsdWUgPSBbIHZhbHVlIF07XG4gICAgICB9IGVsc2UgaWYgKGxlZnQgJiYgIXJpZ2h0KSB7IC8vIElmIG9uZSBvZiB0aGVtIGlzIGVtcHR5LCBhc3NpZ24gdGhlIG90aGVyIG9uZSBhbmQgc29ydCwgdGhlbiBzZXQgdGhlIGZpbmFsIHZhbHVlc1xuICAgICAgICB0aGlzLmNsZWFySG92ZXJWYWx1ZSgpOyAvLyBDbGVhbiB1cFxuICAgICAgICB0aGlzLnNldFJhbmdlVmFsdWUoJ3NlbGVjdGVkVmFsdWUnLCAncmlnaHQnLCB2YWx1ZSk7XG4gICAgICAgIHRoaXMuc29ydFJhbmdlVmFsdWUoJ3NlbGVjdGVkVmFsdWUnKTsgLy8gU29ydFxuXG4gICAgICAgIHRoaXMudmFsdWVGb3JSYW5nZVNob3cgPSB0aGlzLm5vcm1hbGl6ZVJhbmdlVmFsdWUodGhpcy5zZWxlY3RlZFZhbHVlKTtcbiAgICAgICAgdGhpcy5zZXRWYWx1ZSh0aGlzLmNsb25lUmFuZ2VEYXRlKHRoaXMuc2VsZWN0ZWRWYWx1ZSkpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnNldFZhbHVlKHZhbHVlKTtcbiAgICB9XG4gICAgLy8gdGhpcy5zZWxlY3REYXRlLmVtaXQodmFsdWUpO1xuICB9XG5cbiAgZW5hYmxlUHJldk5leHQoZGlyZWN0aW9uOiAncHJldicgfCAnbmV4dCcsIHBhcnRUeXBlPzogUmFuZ2VQYXJ0VHlwZSk6IGJvb2xlYW4ge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIGNvbnN0IFsgc3RhcnQsIGVuZCBdID0gdGhpcy52YWx1ZUZvclJhbmdlU2hvdztcbiAgICAgIGNvbnN0IHNob3dNaWRkbGUgPSAhc3RhcnQuYWRkTW9udGhzKDEpLmlzU2FtZShlbmQsICdtb250aCcpOyAvLyBPbmUgbW9udGggZGlmZiB0aGVuIGRvbid0IHNob3cgbWlkZGxlIHByZXYvbmV4dFxuICAgICAgaWYgKChwYXJ0VHlwZSA9PT0gJ2xlZnQnICYmIGRpcmVjdGlvbiA9PT0gJ25leHQnKSB8fCAocGFydFR5cGUgPT09ICdyaWdodCcgJiYgZGlyZWN0aW9uID09PSAncHJldicpKSB7XG4gICAgICAgIHJldHVybiBzaG93TWlkZGxlO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cbiAgfVxuXG4gIGdldFBhbmVsTW9kZShwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiBQYW5lbE1vZGUge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIHJldHVybiB0aGlzLnBhbmVsTW9kZVsgdGhpcy5nZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlKSBdIGFzIFBhbmVsTW9kZTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMucGFuZWxNb2RlIGFzIFBhbmVsTW9kZTtcbiAgICB9XG4gIH1cblxuICAvLyBHZXQgc2luZ2xlIHZhbHVlIG9yIHBhcnQgdmFsdWUgb2YgYSByYW5nZVxuICBnZXRWYWx1ZShwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiBDYW5keURhdGUge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIHJldHVybiAodGhpcy52YWx1ZSBhcyBDYW5keURhdGVbXSlbIHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSkgXTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMudmFsdWUgYXMgQ2FuZHlEYXRlO1xuICAgIH1cbiAgfVxuXG4gIGdldFZhbHVlQnlTZWxlY3RvcihwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiBDYW5keURhdGUge1xuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIGNvbnN0IHZhbHVlU2hvdyA9IHRoaXMuc2hvd1RpbWVQaWNrZXIgPyB0aGlzLnZhbHVlIDogdGhpcy52YWx1ZUZvclJhbmdlU2hvdzsgLy8gVXNlIHRoZSByZWFsIHRpbWUgdmFsdWUgdGhhdCB3aXRob3V0IGRlY29yYXRpb25zIHdoZW4gdGltZXBpY2tlciBpcyBzaG93biB1cFxuICAgICAgcmV0dXJuICh2YWx1ZVNob3cgYXMgQ2FuZHlEYXRlW10pWyB0aGlzLmdldFBhcnRUeXBlSW5kZXgocGFydFR5cGUpIF07XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB0aGlzLnZhbHVlIGFzIENhbmR5RGF0ZTtcbiAgICB9XG4gIH1cblxuICBnZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlPzogUmFuZ2VQYXJ0VHlwZSk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMucGFydFR5cGVNYXBbIHBhcnRUeXBlISBdO1xuICB9XG5cbiAgZ2V0UGxhY2Vob2xkZXIocGFydFR5cGU/OiBSYW5nZVBhcnRUeXBlKTogc3RyaW5nIHtcbiAgICByZXR1cm4gdGhpcy5pc1JhbmdlID8gdGhpcy5wbGFjZWhvbGRlclsgdGhpcy5nZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlKSBdIDogdGhpcy5wbGFjZWhvbGRlciBhcyBzdHJpbmc7XG4gIH1cblxuICBoYXNTZWxlY3RlZFZhbHVlKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLnNlbGVjdGVkVmFsdWUgJiYgISF0aGlzLnNlbGVjdGVkVmFsdWVbIDEgXSAmJiAhIXRoaXMuc2VsZWN0ZWRWYWx1ZVsgMCBdO1xuICB9XG5cbiAgZGlzYWJsZWRTdGFydFRpbWUgPSAodmFsdWU6IERhdGUgfCBEYXRlW10pOiBEaXNhYmxlZFRpbWVDb25maWcgPT4ge1xuICAgIHJldHVybiB0aGlzLmRpc2FibGVkVGltZSAmJiB0aGlzLmRpc2FibGVkVGltZSh2YWx1ZSwgJ3N0YXJ0Jyk7XG4gIH1cblxuICBkaXNhYmxlZEVuZFRpbWUgPSAodmFsdWU6IERhdGUgfCBEYXRlW10pOiBEaXNhYmxlZFRpbWVDb25maWcgPT4ge1xuICAgIHJldHVybiB0aGlzLmRpc2FibGVkVGltZSAmJiB0aGlzLmRpc2FibGVkVGltZSh2YWx1ZSwgJ2VuZCcpO1xuICB9XG5cbiAgaXNBbGxvd2VkU2VsZWN0ZWRWYWx1ZSgpOiBib29sZWFuIHtcbiAgICBjb25zdCBzZWxlY3RlZFZhbHVlID0gdGhpcy5zZWxlY3RlZFZhbHVlO1xuICAgIGlmIChzZWxlY3RlZFZhbHVlICYmIHNlbGVjdGVkVmFsdWVbIDAgXSAmJiBzZWxlY3RlZFZhbHVlWyAxIF0pIHtcbiAgICAgIHJldHVybiBpc0FsbG93ZWREYXRlKHNlbGVjdGVkVmFsdWVbIDAgXSwgdGhpcy5kaXNhYmxlZERhdGUsIHRoaXMuZGlzYWJsZWRTdGFydFRpbWUpICYmXG4gICAgICAgIGlzQWxsb3dlZERhdGUoc2VsZWN0ZWRWYWx1ZVsgMSBdLCB0aGlzLmRpc2FibGVkRGF0ZSwgdGhpcy5kaXNhYmxlZEVuZFRpbWUpO1xuICAgIH1cbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICB0aW1lUGlja2VyRGlzYWJsZWQoKTogYm9vbGVhbiB7XG4gICAgaWYgKCF0aGlzLmhhc1RpbWVQaWNrZXIpIHtcbiAgICAgIHJldHVybiB0cnVlO1xuICAgIH1cblxuICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIHJldHVybiAhdGhpcy5oYXNTZWxlY3RlZFZhbHVlKCkgfHwgISF0aGlzLmhvdmVyVmFsdWUubGVuZ3RoO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG5cbiAgb2tEaXNhYmxlZCgpOiBib29sZWFuIHtcbiAgICBpZiAoIXRoaXMuaGFzVGltZVBpY2tlcikge1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfVxuXG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgcmV0dXJuICF0aGlzLmlzQWxsb3dlZFNlbGVjdGVkVmFsdWUoKSB8fCAhdGhpcy5oYXNTZWxlY3RlZFZhbHVlKCkgfHwgISF0aGlzLmhvdmVyVmFsdWUubGVuZ3RoO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGhpcy52YWx1ZSA/ICFpc0FsbG93ZWREYXRlKHRoaXMudmFsdWUgYXMgQ2FuZHlEYXRlLCB0aGlzLmRpc2FibGVkRGF0ZSwgdGhpcy5kaXNhYmxlZFRpbWUpIDogZmFsc2U7XG4gICAgfVxuICB9XG5cbiAgZ2V0VGltZU9wdGlvbnMocGFydFR5cGU/OiBSYW5nZVBhcnRUeXBlKTogU3VwcG9ydFRpbWVPcHRpb25zIHwgbnVsbCB7XG4gICAgaWYgKHRoaXMuc2hvd1RpbWUgJiYgdGhpcy50aW1lT3B0aW9ucykge1xuICAgICAgcmV0dXJuIHRoaXMudGltZU9wdGlvbnMgaW5zdGFuY2VvZiBBcnJheVxuICAgICAgICA/IHRoaXMudGltZU9wdGlvbnNbIHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSkgXVxuICAgICAgICA6IHRoaXMudGltZU9wdGlvbnM7XG4gICAgfVxuICAgIHJldHVybiBudWxsO1xuICB9XG5cbiAgb25DbGlja1ByZXNldFJhbmdlKHZhbDogRGF0ZVtdKTogdm9pZCB7XG4gICAgY29uc3QgdmFsdWUgPSB2YWw7XG4gICAgdGhpcy5zZXRWYWx1ZShbIG5ldyBDYW5keURhdGUodmFsdWVbIDAgXSksIG5ldyBDYW5keURhdGUodmFsdWVbIDEgXSkgXSk7XG4gICAgdGhpcy5yZXN1bHRPay5lbWl0KCk7XG4gIH1cblxuICBvblByZXNldFJhbmdlTW91c2VMZWF2ZSgpOiB2b2lkIHtcbiAgICB0aGlzLmNsZWFySG92ZXJWYWx1ZSgpO1xuICB9XG5cbiAgb25Ib3ZlclByZXNldFJhbmdlKHZhbDogRGF0ZVtdKTogdm9pZCB7XG4gICAgdGhpcy5ob3ZlclZhbHVlID0gKFsgbmV3IENhbmR5RGF0ZSh2YWxbIDAgXSksIG5ldyBDYW5keURhdGUodmFsWyAxIF0pIF0pO1xuICB9XG5cbiAgZ2V0T2JqZWN0S2V5cyhvYmo6IG9iamVjdCk6IHN0cmluZ1tdIHtcbiAgICByZXR1cm4gb2JqID8gT2JqZWN0LmtleXMob2JqKSA6IFtdO1xuICB9XG5cbiAgcHJpdmF0ZSBjbG9zZVBpY2tlclBhbmVsKCk6IHZvaWQge1xuICAgIHRoaXMuY2xvc2VQaWNrZXIuZW1pdCgpO1xuICB9XG5cbiAgcHJpdmF0ZSBjbGVhckhvdmVyVmFsdWUoKTogdm9pZCB7XG4gICAgdGhpcy5ob3ZlclZhbHVlID0gW107XG4gIH1cblxuICBwcml2YXRlIGJ1aWxkVGltZU9wdGlvbnMoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuc2hvd1RpbWUpIHtcbiAgICAgIGNvbnN0IHNob3dUaW1lID0gdHlwZW9mIHRoaXMuc2hvd1RpbWUgPT09ICdvYmplY3QnID8gdGhpcy5zaG93VGltZSA6IHt9O1xuICAgICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgICBjb25zdCB2YWx1ZSA9IHRoaXMudmFsdWUgYXMgQ2FuZHlEYXRlW107XG4gICAgICAgIHRoaXMudGltZU9wdGlvbnMgPSBbXG4gICAgICAgICAgdGhpcy5vdmVycmlkZVRpbWVPcHRpb25zKHNob3dUaW1lLCB2YWx1ZVsgMCBdLCAnc3RhcnQnKSxcbiAgICAgICAgICB0aGlzLm92ZXJyaWRlVGltZU9wdGlvbnMoc2hvd1RpbWUsIHZhbHVlWyAxIF0sICdlbmQnKVxuICAgICAgICBdO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy50aW1lT3B0aW9ucyA9IHRoaXMub3ZlcnJpZGVUaW1lT3B0aW9ucyhzaG93VGltZSwgdGhpcy52YWx1ZSBhcyBDYW5keURhdGUpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnRpbWVPcHRpb25zID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIG92ZXJyaWRlVGltZU9wdGlvbnMob3JpZ2luOiBTdXBwb3J0VGltZU9wdGlvbnMsIHZhbHVlOiBDYW5keURhdGUsIHBhcnRpYWw/OiBEaXNhYmxlZFRpbWVQYXJ0aWFsKTogU3VwcG9ydFRpbWVPcHRpb25zIHtcbiAgICBsZXQgZGlzYWJsZWRUaW1lRm47XG4gICAgaWYgKHBhcnRpYWwpIHtcbiAgICAgIGRpc2FibGVkVGltZUZuID0gcGFydGlhbCA9PT0gJ3N0YXJ0JyA/IHRoaXMuZGlzYWJsZWRTdGFydFRpbWUgOiB0aGlzLmRpc2FibGVkRW5kVGltZTtcbiAgICB9IGVsc2Uge1xuICAgICAgZGlzYWJsZWRUaW1lRm4gPSB0aGlzLmRpc2FibGVkVGltZTtcbiAgICB9XG4gICAgcmV0dXJuIHsgLi4ub3JpZ2luLCAuLi5nZXRUaW1lQ29uZmlnKHZhbHVlLCBkaXNhYmxlZFRpbWVGbikgfTtcbiAgfVxuXG4gIC8vIFNldCB2YWx1ZSBhbmQgdHJpZ2dlciBjaGFuZ2UgZXZlbnRcbiAgcHJpdmF0ZSBzZXRWYWx1ZSh2YWx1ZTogQ2FuZHlEYXRlIHwgQ2FuZHlEYXRlW10pOiB2b2lkIHtcbiAgICBjb25zdCBuZXdWYWx1ZSA9IHZhbHVlO1xuXG4gICAgLy8gVE9ETzogU3luYyBvcmlnaW5hbCB0aW1lIChOT1RFOiB0aGlzIHNob3VsZCB0YWtlIG1vcmUgY2FyZSBvZiBiZWFjdXNlIGl0IG1heSBkZXBlbmQgb24gbWFueSBjaGFuZ2Ugc291cmNlcylcbiAgICAvLyBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgLy8gICAvLyBUT0RPOiBTeW5jIHRpbWVcbiAgICAvLyB9IGVsc2Uge1xuICAgIC8vICAgaWYgKHRoaXMudmFsdWUpIHsgLy8gU3luYyB0aW1lIGZyb20gdGhlIG9yaWdpbmFsIG9uZSBpZiBpdCdzIGF2YWlsYWJsZVxuICAgIC8vICAgICBuZXdWYWx1ZSA9IHRoaXMub3ZlcnJpZGVIbXModGhpcy52YWx1ZSBhcyBDYW5keURhdGUsIG5ld1ZhbHVlIGFzIENhbmR5RGF0ZSk7XG4gICAgLy8gICB9XG4gICAgLy8gfVxuXG4gICAgdGhpcy52YWx1ZSA9IG5ld1ZhbHVlO1xuICAgIHRoaXMudmFsdWVDaGFuZ2UuZW1pdCh0aGlzLnZhbHVlKTtcblxuICAgIHRoaXMuYnVpbGRUaW1lT3B0aW9ucygpO1xuICB9XG5cbiAgcHJpdmF0ZSBvdmVycmlkZUhtcyhmcm9tOiBDYW5keURhdGUsIHRvOiBDYW5keURhdGUpOiBDYW5keURhdGUgfCBudWxsIHtcbiAgICBpZiAoIWZyb20gfHwgIXRvKSB7XG4gICAgICByZXR1cm4gbnVsbDtcbiAgICB9XG4gICAgcmV0dXJuIHRvLnNldEhtcyhmcm9tLmdldEhvdXJzKCksIGZyb20uZ2V0TWludXRlcygpLCBmcm9tLmdldFNlY29uZHMoKSk7XG4gIH1cblxuICAvLyBDaGVjayBpZiBpdCdzIGEgdmFsaWQgcmFuZ2UgdmFsdWVcbiAgcHJpdmF0ZSBpc1ZhbGlkUmFuZ2UodmFsdWU6IENhbmR5RGF0ZVtdKTogYm9vbGVhbiB7XG4gICAgaWYgKEFycmF5LmlzQXJyYXkodmFsdWUpKSB7XG4gICAgICBjb25zdCBbIHN0YXJ0LCBlbmQgXSA9IHZhbHVlO1xuICAgICAgY29uc3QgZ3JhaW4gPSB0aGlzLmhhc1RpbWVQaWNrZXIgPyAnc2Vjb25kJyA6ICdkYXknO1xuICAgICAgcmV0dXJuIHN0YXJ0ICYmIGVuZCAmJiAoc3RhcnQuaXNCZWZvcmUoZW5kLCBncmFpbikgfHwgc3RhcnQuaXNTYW1lKGVuZCwgZ3JhaW4pKTtcbiAgICB9XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgcHJpdmF0ZSBub3JtYWxpemVSYW5nZVZhbHVlKHZhbHVlOiBDYW5keURhdGVbXSk6IENhbmR5RGF0ZVtdIHtcbiAgICBjb25zdCBbIHN0YXJ0LCBlbmQgXSA9IHZhbHVlO1xuICAgIGNvbnN0IG5ld1N0YXJ0ID0gc3RhcnQgfHwgbmV3IENhbmR5RGF0ZSgpO1xuICAgIGNvbnN0IG5ld0VuZCA9IGVuZCAmJiBlbmQuaXNTYW1lKG5ld1N0YXJ0LCAnbW9udGgnKSA/IGVuZC5hZGRNb250aHMoMSkgOiBlbmQgfHwgbmV3U3RhcnQuYWRkTW9udGhzKDEpO1xuICAgIHJldHVybiBbIG5ld1N0YXJ0LCBuZXdFbmQgXTtcbiAgfVxuXG4gIC8vIHByaXZhdGUgaXNFbXB0eVJhbmdlVmFsdWUodmFsdWU6IENhbmR5RGF0ZVtdKTogYm9vbGVhbiB7XG4gIC8vICAgcmV0dXJuICF2YWx1ZSB8fCAhQXJyYXkuaXNBcnJheSh2YWx1ZSkgfHwgdmFsdWUuZXZlcnkoKHZhbCkgPT4gIXZhbCk7XG4gIC8vIH1cblxuICAvLyBTb3J0IGEgcmFuZ2UgdmFsdWUgKGFjY3VyYXRlIHRvIHNlY29uZClcbiAgcHJpdmF0ZSBzb3J0UmFuZ2VWYWx1ZShrZXk6ICdzZWxlY3RlZFZhbHVlJyk6IHZvaWQge1xuICAgIGlmIChBcnJheS5pc0FycmF5KHRoaXNbIGtleSBdKSkge1xuICAgICAgY29uc3QgWyBzdGFydCwgZW5kIF0gPSB0aGlzWyBrZXkgXTtcbiAgICAgIGlmIChzdGFydCAmJiBlbmQgJiYgc3RhcnQuaXNBZnRlcihlbmQsICdkYXknKSkge1xuICAgICAgICB0aGlzWyBrZXkgXSA9IFsgZW5kLCBzdGFydCBdO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIFJlbmV3IGFuZCBzZXQgYSByYW5nZSB2YWx1ZSB0byB0cmlnZ2VyIHN1Yi1jb21wb25lbnQncyBjaGFuZ2UgZGV0ZWN0aW9uXG4gIHByaXZhdGUgc2V0UmFuZ2VWYWx1ZShrZXk6ICd2YWx1ZScgfCAnc2VsZWN0ZWRWYWx1ZScsIHBhcnRUeXBlOiBSYW5nZVBhcnRUeXBlLCB2YWx1ZTogQ2FuZHlEYXRlKTogdm9pZCB7XG4gICAgY29uc3QgcmVmID0gdGhpc1sga2V5IF0gPSB0aGlzLmNsb25lUmFuZ2VEYXRlKHRoaXNbIGtleSBdIGFzIENhbmR5RGF0ZVtdKTtcbiAgICByZWZbIHRoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSkgXSA9IHZhbHVlO1xuICB9XG5cbiAgcHJpdmF0ZSBjbG9uZVJhbmdlRGF0ZSh2YWx1ZTogQ2FuZHlEYXRlW10pOiBDYW5keURhdGVbXSB7XG4gICAgcmV0dXJuIFsgdmFsdWVbIDAgXSAmJiB2YWx1ZVsgMCBdLmNsb25lKCksIHZhbHVlWyAxIF0gJiYgdmFsdWVbIDEgXS5jbG9uZSgpIF0gYXMgQ2FuZHlEYXRlW107XG4gIH1cblxuICBwcml2YXRlIGluaXRpYWxBcnJheShrZXk6IHN0cmluZyk6IHZvaWQge1xuICAgIGlmICghdGhpc1sga2V5IF0gfHwgIUFycmF5LmlzQXJyYXkodGhpc1sga2V5IF0pKSB7XG4gICAgICB0aGlzWyBrZXkgXSA9IFtdO1xuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgdHlwZSBSYW5nZVBhcnRUeXBlID0gJ2xlZnQnIHwgJ3JpZ2h0JztcbiJdfQ==