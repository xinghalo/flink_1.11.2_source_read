/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { CandyDate } from '../candy-date';
/** @type {?} */
var MAX_ROW = 4;
/** @type {?} */
var MAX_COL = 3;
var YearPanelComponent = /** @class */ (function () {
    function YearPanelComponent() {
        this.valueChange = new EventEmitter();
        this.decadePanelShow = new EventEmitter();
        this.prefixCls = 'ant-calendar-year-panel';
    }
    Object.defineProperty(YearPanelComponent.prototype, "currentYear", {
        get: /**
         * @return {?}
         */
        function () {
            return this.value.getYear();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(YearPanelComponent.prototype, "startYear", {
        get: /**
         * @return {?}
         */
        function () {
            return parseInt("" + this.currentYear / 10, 10) * 10;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(YearPanelComponent.prototype, "endYear", {
        get: /**
         * @return {?}
         */
        function () {
            return this.startYear + 9;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} changes
     * @return {?}
     */
    YearPanelComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.value || changes.disabledDate) {
            this.render();
        }
    };
    /**
     * @return {?}
     */
    YearPanelComponent.prototype.previousDecade = /**
     * @return {?}
     */
    function () {
        this.gotoYear(-10);
    };
    /**
     * @return {?}
     */
    YearPanelComponent.prototype.nextDecade = /**
     * @return {?}
     */
    function () {
        this.gotoYear(10);
    };
    /**
     * @param {?} _index
     * @param {?} yearData
     * @return {?}
     */
    YearPanelComponent.prototype.trackPanelYear = /**
     * @param {?} _index
     * @param {?} yearData
     * @return {?}
     */
    function (_index, yearData) {
        return yearData.content;
    };
    /**
     * @private
     * @return {?}
     */
    YearPanelComponent.prototype.render = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.value) {
            this.panelYears = this.makePanelYears();
        }
    };
    // Re-render panel content by the header's buttons (NOTE: Do not try to trigger final value change)
    // Re-render panel content by the header's buttons (NOTE: Do not try to trigger final value change)
    /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    YearPanelComponent.prototype.gotoYear = 
    // Re-render panel content by the header's buttons (NOTE: Do not try to trigger final value change)
    /**
     * @private
     * @param {?} amount
     * @return {?}
     */
    function (amount) {
        this.value = this.value.addYears(amount);
        // this.valueChange.emit(this.value); // Do not trigger final value change
        this.render();
    };
    /**
     * @private
     * @param {?} year
     * @return {?}
     */
    YearPanelComponent.prototype.chooseYear = /**
     * @private
     * @param {?} year
     * @return {?}
     */
    function (year) {
        this.value = this.value.setYear(year);
        this.valueChange.emit(this.value);
        this.render();
    };
    /**
     * @private
     * @return {?}
     */
    YearPanelComponent.prototype.makePanelYears = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var years = [];
        /** @type {?} */
        var currentYear = this.currentYear;
        /** @type {?} */
        var startYear = this.startYear;
        /** @type {?} */
        var endYear = this.endYear;
        /** @type {?} */
        var previousYear = startYear - 1;
        /** @type {?} */
        var index = 0;
        for (var rowIndex = 0; rowIndex < MAX_ROW; rowIndex++) {
            years[rowIndex] = [];
            var _loop_1 = function (colIndex) {
                var _a;
                /** @type {?} */
                var year = previousYear + index;
                /** @type {?} */
                var content = String(year);
                /** @type {?} */
                var disabled = this_1.disabledDate ? this_1.disabledDate(this_1.value.setYear(year).nativeDate) : false;
                /** @type {?} */
                var cell = years[rowIndex][colIndex] = {
                    disabled: disabled,
                    content: content,
                    year: year,
                    title: content,
                    isCurrent: year === currentYear,
                    isLowerThanStart: year < startYear,
                    isBiggerThanEnd: year > endYear,
                    classMap: null,
                    onClick: null
                };
                cell.classMap = (_a = {},
                    _a[this_1.prefixCls + "-cell"] = true,
                    _a[this_1.prefixCls + "-selected-cell"] = cell.isCurrent,
                    _a[this_1.prefixCls + "-cell-disabled"] = disabled,
                    _a[this_1.prefixCls + "-last-decade-cell"] = cell.isLowerThanStart,
                    _a[this_1.prefixCls + "-next-decade-cell"] = cell.isBiggerThanEnd,
                    _a);
                if (cell.isLowerThanStart) {
                    cell.onClick = (/**
                     * @return {?}
                     */
                    function () { return _this.previousDecade(); });
                }
                else if (cell.isBiggerThanEnd) {
                    cell.onClick = (/**
                     * @return {?}
                     */
                    function () { return _this.nextDecade(); });
                }
                else {
                    cell.onClick = (/**
                     * @return {?}
                     */
                    function () { return _this.chooseYear(cell.year); });
                }
                index++;
            };
            var this_1 = this;
            for (var colIndex = 0; colIndex < MAX_COL; colIndex++) {
                _loop_1(colIndex);
            }
        }
        return years;
    };
    YearPanelComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    // tslint:disable-next-line:component-selector
                    selector: 'year-panel',
                    template: "<div class=\"{{ prefixCls }}\">\n  <div>\n    <div class=\"{{ prefixCls }}-header\">\n      <a\n        class=\"{{ prefixCls }}-prev-decade-btn\"\n        role=\"button\"\n        (click)=\"previousDecade()\"\n        title=\"{{ locale.previousDecade }}\"\n      ></a>\n      <a\n        class=\"{{ prefixCls }}-decade-select\"\n        role=\"button\"\n        (click)=\"decadePanelShow.emit()\"\n        title=\"{{ locale.decadeSelect }}\"\n      >\n        <span class=\"{{ prefixCls }}-decade-select-content\">\n          {{ startYear }}-{{ endYear }}\n        </span>\n        <span class=\"{{ prefixCls }}-decade-select-arrow\">x</span>\n      </a>\n\n      <a class=\"{{ prefixCls }}-next-decade-btn\" (click)=\"nextDecade()\" title=\"{{ locale.nextDecade }}\" role=\"button\"></a>\n    </div>\n    <div class=\"{{ prefixCls }}-body\">\n      <table class=\"{{ prefixCls }}-table\" cellSpacing=\"0\" role=\"grid\">\n        <tbody class=\"{{ prefixCls }}-tbody\">\n          <tr *ngFor=\"let row of panelYears\" role=\"row\">\n            <td *ngFor=\"let yearCell of row; trackBy: trackPanelYear\"\n              role=\"gridcell\"\n              title=\"{{ yearCell.title }}\"\n              (click)=\"yearCell.disabled ? null : yearCell.onClick()\"\n              [ngClass]=\"yearCell.classMap\"\n            >\n              <a class=\"{{ prefixCls }}-year\">{{ yearCell.content }}</a>\n            </td>\n          </tr>\n        </tbody>\n      </table>\n    </div>\n  </div>\n</div>",
                    styles: [
                        // Support disabledDate
                        "\n      .ant-calendar-year-panel-cell-disabled .ant-calendar-year-panel-year, .ant-calendar-year-panel-cell-disabled .ant-calendar-year-panel-year:hover {\n        color: rgba(0,0,0,0.25);\n        background: #f5f5f5;\n        cursor: not-allowed;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    YearPanelComponent.ctorParameters = function () { return []; };
    YearPanelComponent.propDecorators = {
        locale: [{ type: Input }],
        value: [{ type: Input }],
        valueChange: [{ type: Output }],
        disabledDate: [{ type: Input }],
        decadePanelShow: [{ type: Output }]
    };
    return YearPanelComponent;
}());
export { YearPanelComponent };
if (false) {
    /** @type {?} */
    YearPanelComponent.prototype.locale;
    /** @type {?} */
    YearPanelComponent.prototype.value;
    /** @type {?} */
    YearPanelComponent.prototype.valueChange;
    /** @type {?} */
    YearPanelComponent.prototype.disabledDate;
    /** @type {?} */
    YearPanelComponent.prototype.decadePanelShow;
    /** @type {?} */
    YearPanelComponent.prototype.prefixCls;
    /** @type {?} */
    YearPanelComponent.prototype.panelYears;
}
/**
 * @record
 */
export function PanelYearData() { }
if (false) {
    /** @type {?} */
    PanelYearData.prototype.disabled;
    /** @type {?} */
    PanelYearData.prototype.content;
    /** @type {?} */
    PanelYearData.prototype.year;
    /** @type {?} */
    PanelYearData.prototype.title;
    /** @type {?} */
    PanelYearData.prototype.isCurrent;
    /** @type {?} */
    PanelYearData.prototype.isLowerThanStart;
    /** @type {?} */
    PanelYearData.prototype.isBiggerThanEnd;
    /** @type {?} */
    PanelYearData.prototype.classMap;
    /** @type {?} */
    PanelYearData.prototype.onClick;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieWVhci1wYW5lbC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZGF0ZS1waWNrZXIvbGliL3llYXIveWVhci1wYW5lbC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsWUFBWSxFQUFFLEtBQUssRUFBYSxNQUFNLEVBQWlCLGlCQUFpQixFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRzdJLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxlQUFlLENBQUM7O0lBRXBDLE9BQU8sR0FBRyxDQUFDOztJQUNYLE9BQU8sR0FBRyxDQUFDO0FBRWpCO0lBeUNFO1FBbkJtQixnQkFBVyxHQUFHLElBQUksWUFBWSxFQUFhLENBQUM7UUFJNUMsb0JBQWUsR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO1FBWTlELGNBQVMsR0FBVyx5QkFBeUIsQ0FBQztJQUc5QixDQUFDO0lBYmpCLHNCQUFJLDJDQUFXOzs7O1FBQWY7WUFDRSxPQUFPLElBQUksQ0FBQyxLQUFLLENBQUMsT0FBTyxFQUFFLENBQUM7UUFDOUIsQ0FBQzs7O09BQUE7SUFDRCxzQkFBSSx5Q0FBUzs7OztRQUFiO1lBQ0UsT0FBTyxRQUFRLENBQUMsS0FBRyxJQUFJLENBQUMsV0FBVyxHQUFHLEVBQUksRUFBRSxFQUFFLENBQUMsR0FBRyxFQUFFLENBQUM7UUFDdkQsQ0FBQzs7O09BQUE7SUFDRCxzQkFBSSx1Q0FBTzs7OztRQUFYO1lBQ0UsT0FBTyxJQUFJLENBQUMsU0FBUyxHQUFHLENBQUMsQ0FBQztRQUM1QixDQUFDOzs7T0FBQTs7Ozs7SUFPRCx3Q0FBVzs7OztJQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxPQUFPLENBQUMsS0FBSyxJQUFJLE9BQU8sQ0FBQyxZQUFZLEVBQUU7WUFDekMsSUFBSSxDQUFDLE1BQU0sRUFBRSxDQUFDO1NBQ2Y7SUFDSCxDQUFDOzs7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LGdDQUFnQjs7SUFDaEIsNkJBQWE7O0lBQ2IsOEJBQWM7O0lBQ2Qsa0NBQW1COztJQUNuQix5Q0FBMEI7O0lBQzFCLHdDQUF5Qjs7SUFDekIsaUNBQXdCOztJQUN4QixnQ0FBNkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSwgQ29tcG9uZW50LCBFdmVudEVtaXR0ZXIsIElucHV0LCBPbkNoYW5nZXMsIE91dHB1dCwgU2ltcGxlQ2hhbmdlcywgVmlld0VuY2Fwc3VsYXRpb24gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpDYWxlbmRhckkxOG5JbnRlcmZhY2UgfSBmcm9tICcuLi8uLi8uLi9pMThuL256LWkxOG4uaW50ZXJmYWNlJztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4uL2NhbmR5LWRhdGUnO1xuXG5jb25zdCBNQVhfUk9XID0gNDtcbmNvbnN0IE1BWF9DT0wgPSAzO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpjb21wb25lbnQtc2VsZWN0b3JcbiAgc2VsZWN0b3I6ICd5ZWFyLXBhbmVsJyxcbiAgdGVtcGxhdGVVcmw6ICd5ZWFyLXBhbmVsLmNvbXBvbmVudC5odG1sJyxcbiAgc3R5bGVzOiBbXG4gICAgLy8gU3VwcG9ydCBkaXNhYmxlZERhdGVcbiAgICBgXG4gICAgICAuYW50LWNhbGVuZGFyLXllYXItcGFuZWwtY2VsbC1kaXNhYmxlZCAuYW50LWNhbGVuZGFyLXllYXItcGFuZWwteWVhciwgLmFudC1jYWxlbmRhci15ZWFyLXBhbmVsLWNlbGwtZGlzYWJsZWQgLmFudC1jYWxlbmRhci15ZWFyLXBhbmVsLXllYXI6aG92ZXIge1xuICAgICAgICBjb2xvcjogcmdiYSgwLDAsMCwwLjI1KTtcbiAgICAgICAgYmFja2dyb3VuZDogI2Y1ZjVmNTtcbiAgICAgICAgY3Vyc29yOiBub3QtYWxsb3dlZDtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5cbmV4cG9ydCBjbGFzcyBZZWFyUGFuZWxDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBsb2NhbGU6IE56Q2FsZW5kYXJJMThuSW50ZXJmYWNlO1xuXG4gIEBJbnB1dCgpIHZhbHVlOiBDYW5keURhdGU7XG4gIEBPdXRwdXQoKSByZWFkb25seSB2YWx1ZUNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Q2FuZHlEYXRlPigpO1xuXG4gIEBJbnB1dCgpIGRpc2FibGVkRGF0ZTogKGRhdGU6IERhdGUpID0+IGJvb2xlYW47XG5cbiAgQE91dHB1dCgpIHJlYWRvbmx5IGRlY2FkZVBhbmVsU2hvdyA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcblxuICBnZXQgY3VycmVudFllYXIoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy52YWx1ZS5nZXRZZWFyKCk7XG4gIH1cbiAgZ2V0IHN0YXJ0WWVhcigpOiBudW1iZXIge1xuICAgIHJldHVybiBwYXJzZUludChgJHt0aGlzLmN1cnJlbnRZZWFyIC8gMTB9YCwgMTApICogMTA7XG4gIH1cbiAgZ2V0IGVuZFllYXIoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5zdGFydFllYXIgKyA5O1xuICB9XG5cbiAgcHJlZml4Q2xzOiBzdHJpbmcgPSAnYW50LWNhbGVuZGFyLXllYXItcGFuZWwnO1xuICBwYW5lbFllYXJzOiBQYW5lbFllYXJEYXRhW11bXTtcblxuICBjb25zdHJ1Y3RvcigpIHsgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy52YWx1ZSB8fCBjaGFuZ2VzLmRpc2FibGVkRGF0ZSkge1xuICAgICAgdGhpcy5yZW5kZXIoKTtcbiAgICB9XG4gIH1cblxuICBwcmV2aW91c0RlY2FkZSgpOiB2b2lkIHtcbiAgICB0aGlzLmdvdG9ZZWFyKC0xMCk7XG4gIH1cblxuICBuZXh0RGVjYWRlKCk6IHZvaWQge1xuICAgIHRoaXMuZ290b1llYXIoMTApO1xuICB9XG5cbiAgdHJhY2tQYW5lbFllYXIoX2luZGV4OiBudW1iZXIsIHllYXJEYXRhOiBQYW5lbFllYXJEYXRhKTogc3RyaW5nIHtcbiAgICByZXR1cm4geWVhckRhdGEuY29udGVudDtcbiAgfVxuXG4gIHByaXZhdGUgcmVuZGVyKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnZhbHVlKSB7XG4gICAgICB0aGlzLnBhbmVsWWVhcnMgPSB0aGlzLm1ha2VQYW5lbFllYXJzKCk7XG4gICAgfVxuICB9XG5cbiAgLy8gUmUtcmVuZGVyIHBhbmVsIGNvbnRlbnQgYnkgdGhlIGhlYWRlcidzIGJ1dHRvbnMgKE5PVEU6IERvIG5vdCB0cnkgdG8gdHJpZ2dlciBmaW5hbCB2YWx1ZSBjaGFuZ2UpXG4gIHByaXZhdGUgZ290b1llYXIoYW1vdW50OiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLnZhbHVlID0gdGhpcy52YWx1ZS5hZGRZZWFycyhhbW91bnQpO1xuICAgIC8vIHRoaXMudmFsdWVDaGFuZ2UuZW1pdCh0aGlzLnZhbHVlKTsgLy8gRG8gbm90IHRyaWdnZXIgZmluYWwgdmFsdWUgY2hhbmdlXG4gICAgdGhpcy5yZW5kZXIoKTtcbiAgfVxuXG4gIHByaXZhdGUgY2hvb3NlWWVhcih5ZWFyOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLnZhbHVlID0gdGhpcy52YWx1ZS5zZXRZZWFyKHllYXIpO1xuICAgIHRoaXMudmFsdWVDaGFuZ2UuZW1pdCh0aGlzLnZhbHVlKTtcbiAgICB0aGlzLnJlbmRlcigpO1xuICB9XG5cbiAgcHJpdmF0ZSBtYWtlUGFuZWxZZWFycygpOiBQYW5lbFllYXJEYXRhW11bXSB7XG4gICAgY29uc3QgeWVhcnM6IFBhbmVsWWVhckRhdGFbXVtdID0gW107XG4gICAgY29uc3QgY3VycmVudFllYXIgPSB0aGlzLmN1cnJlbnRZZWFyO1xuICAgIGNvbnN0IHN0YXJ0WWVhciA9IHRoaXMuc3RhcnRZZWFyO1xuICAgIGNvbnN0IGVuZFllYXIgPSB0aGlzLmVuZFllYXI7XG4gICAgY29uc3QgcHJldmlvdXNZZWFyID0gc3RhcnRZZWFyIC0gMTtcbiAgICBsZXQgaW5kZXggPSAwO1xuICAgIGZvciAobGV0IHJvd0luZGV4ID0gMDsgcm93SW5kZXggPCBNQVhfUk9XOyByb3dJbmRleCArKykge1xuICAgICAgeWVhcnNbcm93SW5kZXhdID0gW107XG4gICAgICBmb3IgKGxldCBjb2xJbmRleCA9IDA7IGNvbEluZGV4IDwgTUFYX0NPTDsgY29sSW5kZXggKyspIHtcbiAgICAgICAgY29uc3QgeWVhciA9IHByZXZpb3VzWWVhciArIGluZGV4O1xuICAgICAgICBjb25zdCBjb250ZW50ID0gU3RyaW5nKHllYXIpO1xuICAgICAgICBjb25zdCBkaXNhYmxlZCA9IHRoaXMuZGlzYWJsZWREYXRlID8gdGhpcy5kaXNhYmxlZERhdGUodGhpcy52YWx1ZS5zZXRZZWFyKHllYXIpLm5hdGl2ZURhdGUpIDogZmFsc2U7XG5cbiAgICAgICAgY29uc3QgY2VsbDogUGFuZWxZZWFyRGF0YSA9IHllYXJzW3Jvd0luZGV4XVtjb2xJbmRleF0gPSB7XG4gICAgICAgICAgZGlzYWJsZWQsXG4gICAgICAgICAgY29udGVudCxcbiAgICAgICAgICB5ZWFyLFxuICAgICAgICAgIHRpdGxlOiBjb250ZW50LFxuICAgICAgICAgIGlzQ3VycmVudDogeWVhciA9PT0gY3VycmVudFllYXIsXG4gICAgICAgICAgaXNMb3dlclRoYW5TdGFydDogeWVhciA8IHN0YXJ0WWVhcixcbiAgICAgICAgICBpc0JpZ2dlclRoYW5FbmQ6IHllYXIgPiBlbmRZZWFyLFxuICAgICAgICAgIGNsYXNzTWFwOiBudWxsLFxuICAgICAgICAgIG9uQ2xpY2s6IG51bGxcbiAgICAgICAgfTtcblxuICAgICAgICBjZWxsLmNsYXNzTWFwID0ge1xuICAgICAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tY2VsbGBdOiB0cnVlLFxuICAgICAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tc2VsZWN0ZWQtY2VsbGBdOiBjZWxsLmlzQ3VycmVudCxcbiAgICAgICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWNlbGwtZGlzYWJsZWRgXTogZGlzYWJsZWQsXG4gICAgICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1sYXN0LWRlY2FkZS1jZWxsYF06IGNlbGwuaXNMb3dlclRoYW5TdGFydCxcbiAgICAgICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LW5leHQtZGVjYWRlLWNlbGxgXTogY2VsbC5pc0JpZ2dlclRoYW5FbmRcbiAgICAgICAgfTtcblxuICAgICAgICBpZiAoY2VsbC5pc0xvd2VyVGhhblN0YXJ0KSB7XG4gICAgICAgICAgY2VsbC5vbkNsaWNrID0gKCkgPT4gdGhpcy5wcmV2aW91c0RlY2FkZSgpO1xuICAgICAgICB9IGVsc2UgaWYgKGNlbGwuaXNCaWdnZXJUaGFuRW5kKSB7XG4gICAgICAgICAgY2VsbC5vbkNsaWNrID0gKCkgPT4gdGhpcy5uZXh0RGVjYWRlKCk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgY2VsbC5vbkNsaWNrID0gKCkgPT4gdGhpcy5jaG9vc2VZZWFyKGNlbGwueWVhcik7XG4gICAgICAgIH1cblxuICAgICAgICBpbmRleCArKztcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHllYXJzO1xuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgUGFuZWxZZWFyRGF0YSB7XG4gIGRpc2FibGVkOiBib29sZWFuO1xuICBjb250ZW50OiBzdHJpbmc7XG4gIHllYXI6IG51bWJlcjtcbiAgdGl0bGU6IHN0cmluZztcbiAgaXNDdXJyZW50OiBib29sZWFuO1xuICBpc0xvd2VyVGhhblN0YXJ0OiBib29sZWFuO1xuICBpc0JpZ2dlclRoYW5FbmQ6IGJvb2xlYW47XG4gIGNsYXNzTWFwOiBvYmplY3QgfCBudWxsO1xuICBvbkNsaWNrOiBWb2lkRnVuY3Rpb24gfCBudWxsO1xufVxuIl19