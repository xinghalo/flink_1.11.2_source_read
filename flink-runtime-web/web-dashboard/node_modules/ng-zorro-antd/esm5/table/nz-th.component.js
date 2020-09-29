/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { isNotNil } from '../core/util/check';
import { InputBoolean } from '../core/util/convert';
import { NzDropDownComponent } from '../dropdown/nz-dropdown.component';
import { NzI18nService } from '../i18n/nz-i18n.service';
/**
 * @record
 */
export function NzThItemInterface() { }
if (false) {
    /** @type {?} */
    NzThItemInterface.prototype.text;
    /** @type {?} */
    NzThItemInterface.prototype.value;
    /** @type {?} */
    NzThItemInterface.prototype.checked;
}
var NzThComponent = /** @class */ (function () {
    function NzThComponent(cdr, i18n) {
        this.cdr = cdr;
        this.i18n = i18n;
        this.hasFilterValue = false;
        this.filterVisible = false;
        this.multipleFilterList = [];
        this.singleFilterList = [];
        /* tslint:disable-next-line:no-any */
        this.locale = (/** @type {?} */ ({}));
        this.nzWidthChange$ = new Subject();
        this.destroy$ = new Subject();
        this.hasDefaultFilter = false;
        /* tslint:disable-next-line:no-any */
        this.nzSelections = [];
        this.nzChecked = false;
        this.nzDisabled = false;
        this.nzIndeterminate = false;
        this.nzFilterMultiple = true;
        this.nzSort = null;
        this.nzFilters = [];
        this.nzExpand = false;
        this.nzShowCheckbox = false;
        this.nzCustomFilter = false;
        this.nzShowSort = false;
        this.nzShowFilter = false;
        this.nzShowRowSelection = false;
        this.nzCheckedChange = new EventEmitter();
        this.nzSortChange = new EventEmitter();
        this.nzSortChangeWithKey = new EventEmitter();
        /* tslint:disable-next-line:no-any */
        this.nzFilterChange = new EventEmitter();
    }
    /**
     * @return {?}
     */
    NzThComponent.prototype.updateSortValue = /**
     * @return {?}
     */
    function () {
        if (this.nzShowSort) {
            if (this.nzSort === 'descend') {
                this.setSortValue('ascend');
            }
            else if (this.nzSort === 'ascend') {
                this.setSortValue(null);
            }
            else {
                this.setSortValue('descend');
            }
        }
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzThComponent.prototype.setSortValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.nzSort = value;
        this.nzSortChangeWithKey.emit({ key: this.nzSortKey, value: this.nzSort });
        this.nzSortChange.emit(this.nzSort);
    };
    Object.defineProperty(NzThComponent.prototype, "filterList", {
        get: /**
         * @return {?}
         */
        function () {
            return this.multipleFilterList.filter((/**
             * @param {?} item
             * @return {?}
             */
            function (item) { return item.checked; })).map((/**
             * @param {?} item
             * @return {?}
             */
            function (item) { return item.value; }));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzThComponent.prototype, "filterValue", {
        /* tslint:disable-next-line:no-any */
        get: /* tslint:disable-next-line:no-any */
        /**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var checkedFilter = this.singleFilterList.find((/**
             * @param {?} item
             * @return {?}
             */
            function (item) { return item.checked; }));
            return checkedFilter ? checkedFilter.value : null;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzThComponent.prototype.updateFilterStatus = /**
     * @return {?}
     */
    function () {
        if (this.nzFilterMultiple) {
            this.hasFilterValue = this.filterList.length > 0;
        }
        else {
            this.hasFilterValue = isNotNil(this.filterValue);
        }
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.search = /**
     * @return {?}
     */
    function () {
        this.updateFilterStatus();
        if (this.nzFilterMultiple) {
            this.nzFilterChange.emit(this.filterList);
        }
        else {
            this.nzFilterChange.emit(this.filterValue);
        }
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.reset = /**
     * @return {?}
     */
    function () {
        this.initMultipleFilterList(true);
        this.initSingleFilterList(true);
        this.hasFilterValue = false;
    };
    /**
     * @param {?} filter
     * @return {?}
     */
    NzThComponent.prototype.checkMultiple = /**
     * @param {?} filter
     * @return {?}
     */
    function (filter) {
        filter.checked = !filter.checked;
    };
    /**
     * @param {?} filter
     * @return {?}
     */
    NzThComponent.prototype.checkSingle = /**
     * @param {?} filter
     * @return {?}
     */
    function (filter) {
        this.singleFilterList.forEach((/**
         * @param {?} item
         * @return {?}
         */
        function (item) { return (item.checked = item === filter); }));
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.hideDropDown = /**
     * @return {?}
     */
    function () {
        this.nzDropDownComponent.setVisibleStateWhen(false);
        this.filterVisible = false;
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzThComponent.prototype.dropDownVisibleChange = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        this.filterVisible = value;
        if (!value) {
            this.search();
        }
    };
    /**
     * @param {?=} force
     * @return {?}
     */
    NzThComponent.prototype.initMultipleFilterList = /**
     * @param {?=} force
     * @return {?}
     */
    function (force) {
        var _this = this;
        this.multipleFilterList = this.nzFilters.map((/**
         * @param {?} item
         * @return {?}
         */
        function (item) {
            /** @type {?} */
            var checked = force ? false : !!item.byDefault;
            if (checked) {
                _this.hasDefaultFilter = true;
            }
            return { text: item.text, value: item.value, checked: checked };
        }));
        this.checkDefaultFilters();
    };
    /**
     * @param {?=} force
     * @return {?}
     */
    NzThComponent.prototype.initSingleFilterList = /**
     * @param {?=} force
     * @return {?}
     */
    function (force) {
        var _this = this;
        this.singleFilterList = this.nzFilters.map((/**
         * @param {?} item
         * @return {?}
         */
        function (item) {
            /** @type {?} */
            var checked = force ? false : !!item.byDefault;
            if (checked) {
                _this.hasDefaultFilter = true;
            }
            return { text: item.text, value: item.value, checked: checked };
        }));
        this.checkDefaultFilters();
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.checkDefaultFilters = /**
     * @return {?}
     */
    function () {
        if (!this.nzFilters || this.nzFilters.length === 0 || !this.hasDefaultFilter) {
            return;
        }
        this.updateFilterStatus();
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.marForCheck = /**
     * @return {?}
     */
    function () {
        this.cdr.markForCheck();
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.i18n.localeChange.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @return {?}
         */
        function () {
            _this.locale = _this.i18n.getLocaleData('Table');
            _this.cdr.markForCheck();
        }));
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzThComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzFilters) {
            this.initMultipleFilterList();
            this.initSingleFilterList();
            this.updateFilterStatus();
        }
        if (changes.nzWidth) {
            this.nzWidthChange$.next(this.nzWidth);
        }
    };
    /**
     * @return {?}
     */
    NzThComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzThComponent.decorators = [
        { type: Component, args: [{
                    // tslint:disable-next-line:component-selector
                    selector: 'th:not(.nz-disable-th)',
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    template: "<ng-template #checkboxTemplate>\n  <label nz-checkbox\n    [class.ant-table-selection-select-all-custom]=\"nzShowRowSelection\"\n    [(ngModel)]=\"nzChecked\"\n    [nzDisabled]=\"nzDisabled\"\n    [nzIndeterminate]=\"nzIndeterminate\"\n    (ngModelChange)=\"nzCheckedChange.emit($event)\">\n  </label>\n</ng-template>\n<div [class.ant-table-column-sorters]=\"nzShowSort\" (click)=\"updateSortValue()\">\n  <div class=\"ant-table-selection\" *ngIf=\"nzShowRowSelection\">\n    <ng-container *ngIf=\"nzShowCheckbox\">\n      <ng-template [ngTemplateOutlet]=\"checkboxTemplate\"></ng-template>\n    </ng-container>\n    <nz-dropdown nzPlacement=\"bottomLeft\">\n      <div nz-dropdown class=\"ant-table-selection-down\">\n        <i nz-icon type=\"down\"></i>\n      </div>\n      <ul nz-menu class=\"ant-table-selection-menu\">\n        <li nz-menu-item *ngFor=\"let selection of nzSelections\" (click)=\"selection.onSelect()\">{{selection.text}}</li>\n      </ul>\n    </nz-dropdown>\n  </div>\n  <ng-container *ngIf=\"nzShowCheckbox && !nzShowRowSelection\">\n    <ng-template [ngTemplateOutlet]=\"checkboxTemplate\"></ng-template>\n  </ng-container>\n  <ng-content></ng-content>\n  <div class=\"ant-table-column-sorter\" *ngIf=\"nzShowSort\">\n    <i nz-icon\n      type=\"caret-up\"\n      class=\"ant-table-column-sorter-up\"\n      [class.on]=\"nzSort == 'ascend'\"\n      [class.off]=\"nzSort != 'ascend'\"></i>\n    <i nz-icon\n      type=\"caret-down\"\n      class=\"ant-table-column-sorter-down\"\n      [class.on]=\"nzSort == 'descend'\"\n      [class.off]=\"nzSort != 'descend'\"></i>\n  </div>\n</div>\n<nz-dropdown nzTrigger=\"click\" *ngIf=\"nzShowFilter\" [nzClickHide]=\"false\" nzTableFilter (nzVisibleChange)=\"dropDownVisibleChange($event)\">\n  <i nz-icon type=\"filter\" theme=\"fill\" [class.ant-table-filter-selected]=\"hasFilterValue\" [class.ant-table-filter-open]=\"filterVisible\" nz-dropdown></i>\n  <ul nz-menu>\n    <ng-container *ngIf=\"nzFilterMultiple\">\n      <li nz-menu-item *ngFor=\"let filter of multipleFilterList\" (click)=\"checkMultiple(filter)\">\n        <label nz-checkbox [ngModel]=\"filter.checked\" (ngModelChange)=\"checkMultiple(filter)\"></label><span>{{filter.text}}</span>\n      </li>\n    </ng-container>\n    <ng-container *ngIf=\"!nzFilterMultiple\">\n      <li nz-menu-item *ngFor=\"let filter of singleFilterList\" (click)=\"checkSingle(filter)\">\n        <label nz-radio [ngModel]=\"filter.checked\" (ngModelChange)=\"checkSingle(filter)\">{{filter.text}}</label>\n      </li>\n    </ng-container>\n  </ul>\n  <div class=\"ant-table-filter-dropdown-btns\">\n    <a class=\"ant-table-filter-dropdown-link confirm\" (click)=\"hideDropDown()\">\n      <span>{{ locale.filterConfirm }}</span>\n    </a>\n    <a class=\"ant-table-filter-dropdown-link clear\" (click)=\"reset();hideDropDown()\">\n      <span>{{ locale.filterReset }}</span>\n    </a>\n  </div>\n</nz-dropdown>\n",
                    host: {
                        '[class.ant-table-column-has-actions]': 'nzShowFilter || nzShowSort || nzCustomFilter',
                        '[class.ant-table-column-has-filters]': 'nzShowFilter || nzCustomFilter',
                        '[class.ant-table-column-has-sorters]': 'nzShowSort',
                        '[class.ant-table-selection-column-custom]': 'nzShowRowSelection',
                        '[class.ant-table-selection-column]': 'nzShowCheckbox',
                        '[class.ant-table-expand-icon-th]': 'nzExpand',
                        '[class.ant-table-th-left-sticky]': 'nzLeft',
                        '[class.ant-table-th-right-sticky]': 'nzRight',
                        '[class.ant-table-column-sort]': "nzSort === 'descend' || nzSort === 'ascend'",
                        '[style.left]': 'nzLeft',
                        '[style.right]': 'nzRight',
                        '[style.text-align]': 'nzAlign'
                    }
                }] }
    ];
    /** @nocollapse */
    NzThComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef },
        { type: NzI18nService }
    ]; };
    NzThComponent.propDecorators = {
        nzDropDownComponent: [{ type: ViewChild, args: [NzDropDownComponent,] }],
        nzSelections: [{ type: Input }],
        nzChecked: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        nzIndeterminate: [{ type: Input }],
        nzSortKey: [{ type: Input }],
        nzFilterMultiple: [{ type: Input }],
        nzWidth: [{ type: Input }],
        nzLeft: [{ type: Input }],
        nzRight: [{ type: Input }],
        nzAlign: [{ type: Input }],
        nzSort: [{ type: Input }],
        nzFilters: [{ type: Input }],
        nzExpand: [{ type: Input }],
        nzShowCheckbox: [{ type: Input }],
        nzCustomFilter: [{ type: Input }],
        nzShowSort: [{ type: Input }],
        nzShowFilter: [{ type: Input }],
        nzShowRowSelection: [{ type: Input }],
        nzCheckedChange: [{ type: Output }],
        nzSortChange: [{ type: Output }],
        nzSortChangeWithKey: [{ type: Output }],
        nzFilterChange: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzExpand", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzShowCheckbox", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzCustomFilter", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzShowSort", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzShowFilter", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzThComponent.prototype, "nzShowRowSelection", void 0);
    return NzThComponent;
}());
export { NzThComponent };
if (false) {
    /** @type {?} */
    NzThComponent.prototype.hasFilterValue;
    /** @type {?} */
    NzThComponent.prototype.filterVisible;
    /** @type {?} */
    NzThComponent.prototype.multipleFilterList;
    /** @type {?} */
    NzThComponent.prototype.singleFilterList;
    /** @type {?} */
    NzThComponent.prototype.locale;
    /** @type {?} */
    NzThComponent.prototype.nzWidthChange$;
    /**
     * @type {?}
     * @private
     */
    NzThComponent.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzThComponent.prototype.hasDefaultFilter;
    /** @type {?} */
    NzThComponent.prototype.nzDropDownComponent;
    /** @type {?} */
    NzThComponent.prototype.nzSelections;
    /** @type {?} */
    NzThComponent.prototype.nzChecked;
    /** @type {?} */
    NzThComponent.prototype.nzDisabled;
    /** @type {?} */
    NzThComponent.prototype.nzIndeterminate;
    /** @type {?} */
    NzThComponent.prototype.nzSortKey;
    /** @type {?} */
    NzThComponent.prototype.nzFilterMultiple;
    /** @type {?} */
    NzThComponent.prototype.nzWidth;
    /** @type {?} */
    NzThComponent.prototype.nzLeft;
    /** @type {?} */
    NzThComponent.prototype.nzRight;
    /** @type {?} */
    NzThComponent.prototype.nzAlign;
    /** @type {?} */
    NzThComponent.prototype.nzSort;
    /** @type {?} */
    NzThComponent.prototype.nzFilters;
    /** @type {?} */
    NzThComponent.prototype.nzExpand;
    /** @type {?} */
    NzThComponent.prototype.nzShowCheckbox;
    /** @type {?} */
    NzThComponent.prototype.nzCustomFilter;
    /** @type {?} */
    NzThComponent.prototype.nzShowSort;
    /** @type {?} */
    NzThComponent.prototype.nzShowFilter;
    /** @type {?} */
    NzThComponent.prototype.nzShowRowSelection;
    /** @type {?} */
    NzThComponent.prototype.nzCheckedChange;
    /** @type {?} */
    NzThComponent.prototype.nzSortChange;
    /** @type {?} */
    NzThComponent.prototype.nzSortChangeWithKey;
    /** @type {?} */
    NzThComponent.prototype.nzFilterChange;
    /**
     * @type {?}
     * @private
     */
    NzThComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzThComponent.prototype.i18n;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGguY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRoLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUlMLE1BQU0sRUFFTixTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxPQUFPLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDL0IsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzNDLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUM5QyxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFFeEUsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDOzs7O0FBS3hELHVDQUtDOzs7SUFKQyxpQ0FBYTs7SUFFYixrQ0FBVzs7SUFDWCxvQ0FBaUI7O0FBR25CO0lBa0tFLHVCQUFvQixHQUFzQixFQUFVLElBQW1CO1FBQW5ELFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBQVUsU0FBSSxHQUFKLElBQUksQ0FBZTtRQTNJdkUsbUJBQWMsR0FBRyxLQUFLLENBQUM7UUFDdkIsa0JBQWEsR0FBRyxLQUFLLENBQUM7UUFDdEIsdUJBQWtCLEdBQXdCLEVBQUUsQ0FBQztRQUM3QyxxQkFBZ0IsR0FBd0IsRUFBRSxDQUFDOztRQUUzQyxXQUFNLEdBQTZCLG1CQUFBLEVBQUUsRUFBNEIsQ0FBQztRQUNsRSxtQkFBYyxHQUFHLElBQUksT0FBTyxFQUFFLENBQUM7UUFDdkIsYUFBUSxHQUFHLElBQUksT0FBTyxFQUFFLENBQUM7UUFDekIscUJBQWdC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S1ksYUFBYTs7O0lBQ3hCLHVDQUF1Qjs7SUFDdkIsc0NBQXNCOztJQUN0QiwyQ0FBNkM7O0lBQzdDLHlDQUEyQzs7SUFFM0MsK0JBQWtFOztJQUNsRSx1Q0FBK0I7Ozs7O0lBQy9CLGlDQUFpQzs7Ozs7SUFDakMseUNBQWlDOztJQUNqQyw0Q0FBeUU7O0lBRXpFLHFDQUFtRjs7SUFDbkYsa0NBQTJCOztJQUMzQixtQ0FBNEI7O0lBQzVCLHdDQUFpQzs7SUFDakMsa0NBQTJCOztJQUMzQix5Q0FBaUM7O0lBQ2pDLGdDQUF5Qjs7SUFDekIsK0JBQXdCOztJQUN4QixnQ0FBeUI7O0lBQ3pCLGdDQUE4Qzs7SUFDOUMsK0JBQW9EOztJQUNwRCxrQ0FBd0M7O0lBQ3hDLGlDQUEwQzs7SUFDMUMsdUNBQWdEOztJQUNoRCx1Q0FBZ0Q7O0lBQ2hELG1DQUE0Qzs7SUFDNUMscUNBQThDOztJQUM5QywyQ0FBb0Q7O0lBQ3BELHdDQUFpRTs7SUFDakUscUNBQW9FOztJQUNwRSw0Q0FBbUc7O0lBRW5HLHVDQUFvRTs7Ozs7SUEwR3hELDRCQUE4Qjs7Ozs7SUFBRSw2QkFBMkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgaXNOb3ROaWwgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpEcm9wRG93bkNvbXBvbmVudCB9IGZyb20gJy4uL2Ryb3Bkb3duL256LWRyb3Bkb3duLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekkxOG5JbnRlcmZhY2UgfSBmcm9tICcuLi9pMThuL256LWkxOG4uaW50ZXJmYWNlJztcbmltcG9ydCB7IE56STE4blNlcnZpY2UgfSBmcm9tICcuLi9pMThuL256LWkxOG4uc2VydmljZSc7XG5cbi8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbmV4cG9ydCB0eXBlIE56VGhGaWx0ZXJUeXBlID0gQXJyYXk8eyB0ZXh0OiBzdHJpbmc7IHZhbHVlOiBhbnk7IGJ5RGVmYXVsdD86IGJvb2xlYW4gfT47XG5cbmV4cG9ydCBpbnRlcmZhY2UgTnpUaEl0ZW1JbnRlcmZhY2Uge1xuICB0ZXh0OiBzdHJpbmc7XG4gIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbiAgdmFsdWU6IGFueTtcbiAgY2hlY2tlZDogYm9vbGVhbjtcbn1cblxuQENvbXBvbmVudCh7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpjb21wb25lbnQtc2VsZWN0b3JcbiAgc2VsZWN0b3I6ICd0aDpub3QoLm56LWRpc2FibGUtdGgpJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdGguY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtY29sdW1uLWhhcy1hY3Rpb25zXSc6ICduelNob3dGaWx0ZXIgfHwgbnpTaG93U29ydCB8fCBuekN1c3RvbUZpbHRlcicsXG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtY29sdW1uLWhhcy1maWx0ZXJzXSc6ICduelNob3dGaWx0ZXIgfHwgbnpDdXN0b21GaWx0ZXInLFxuICAgICdbY2xhc3MuYW50LXRhYmxlLWNvbHVtbi1oYXMtc29ydGVyc10nOiAnbnpTaG93U29ydCcsXG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtc2VsZWN0aW9uLWNvbHVtbi1jdXN0b21dJzogJ256U2hvd1Jvd1NlbGVjdGlvbicsXG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtc2VsZWN0aW9uLWNvbHVtbl0nOiAnbnpTaG93Q2hlY2tib3gnLFxuICAgICdbY2xhc3MuYW50LXRhYmxlLWV4cGFuZC1pY29uLXRoXSc6ICduekV4cGFuZCcsXG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtdGgtbGVmdC1zdGlja3ldJzogJ256TGVmdCcsXG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtdGgtcmlnaHQtc3RpY2t5XSc6ICduelJpZ2h0JyxcbiAgICAnW2NsYXNzLmFudC10YWJsZS1jb2x1bW4tc29ydF0nOiBgbnpTb3J0ID09PSAnZGVzY2VuZCcgfHwgbnpTb3J0ID09PSAnYXNjZW5kJ2AsXG4gICAgJ1tzdHlsZS5sZWZ0XSc6ICduekxlZnQnLFxuICAgICdbc3R5bGUucmlnaHRdJzogJ256UmlnaHQnLFxuICAgICdbc3R5bGUudGV4dC1hbGlnbl0nOiAnbnpBbGlnbidcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelRoQ29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzLCBPbkluaXQsIE9uRGVzdHJveSB7XG4gIGhhc0ZpbHRlclZhbHVlID0gZmFsc2U7XG4gIGZpbHRlclZpc2libGUgPSBmYWxzZTtcbiAgbXVsdGlwbGVGaWx0ZXJMaXN0OiBOelRoSXRlbUludGVyZmFjZVtdID0gW107XG4gIHNpbmdsZUZpbHRlckxpc3Q6IE56VGhJdGVtSW50ZXJmYWNlW10gPSBbXTtcbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICBsb2NhbGU6IE56STE4bkludGVyZmFjZVsnVGFibGUnXSA9IHt9IGFzIE56STE4bkludGVyZmFjZVsnVGFibGUnXTtcbiAgbnpXaWR0aENoYW5nZSQgPSBuZXcgU3ViamVjdCgpO1xuICBwcml2YXRlIGRlc3Ryb3kkID0gbmV3IFN1YmplY3QoKTtcbiAgcHJpdmF0ZSBoYXNEZWZhdWx0RmlsdGVyID0gZmFsc2U7XG4gIEBWaWV3Q2hpbGQoTnpEcm9wRG93bkNvbXBvbmVudCkgbnpEcm9wRG93bkNvbXBvbmVudDogTnpEcm9wRG93bkNvbXBvbmVudDtcbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICBASW5wdXQoKSBuelNlbGVjdGlvbnM6IEFycmF5PHsgdGV4dDogc3RyaW5nOyBvblNlbGVjdCguLi5hcmdzOiBhbnlbXSk6IGFueSB9PiA9IFtdO1xuICBASW5wdXQoKSBuekNoZWNrZWQgPSBmYWxzZTtcbiAgQElucHV0KCkgbnpEaXNhYmxlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBuekluZGV0ZXJtaW5hdGUgPSBmYWxzZTtcbiAgQElucHV0KCkgbnpTb3J0S2V5OiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56RmlsdGVyTXVsdGlwbGUgPSB0cnVlO1xuICBASW5wdXQoKSBueldpZHRoOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56TGVmdDogc3RyaW5nO1xuICBASW5wdXQoKSBuelJpZ2h0OiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56QWxpZ246ICdsZWZ0JyB8ICdyaWdodCcgfCAnY2VudGVyJztcbiAgQElucHV0KCkgbnpTb3J0OiAnYXNjZW5kJyB8ICdkZXNjZW5kJyB8IG51bGwgPSBudWxsO1xuICBASW5wdXQoKSBuekZpbHRlcnM6IE56VGhGaWx0ZXJUeXBlID0gW107XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekV4cGFuZCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93Q2hlY2tib3ggPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Q3VzdG9tRmlsdGVyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dTb3J0ID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dGaWx0ZXIgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1Jvd1NlbGVjdGlvbiA9IGZhbHNlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpDaGVja2VkQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTb3J0Q2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxzdHJpbmcgfCBudWxsPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTb3J0Q2hhbmdlV2l0aEtleSA9IG5ldyBFdmVudEVtaXR0ZXI8eyBrZXk6IHN0cmluZzsgdmFsdWU6IHN0cmluZyB8IG51bGwgfT4oKTtcbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpGaWx0ZXJDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPGFueVtdIHwgYW55PigpO1xuXG4gIHVwZGF0ZVNvcnRWYWx1ZSgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelNob3dTb3J0KSB7XG4gICAgICBpZiAodGhpcy5uelNvcnQgPT09ICdkZXNjZW5kJykge1xuICAgICAgICB0aGlzLnNldFNvcnRWYWx1ZSgnYXNjZW5kJyk7XG4gICAgICB9IGVsc2UgaWYgKHRoaXMubnpTb3J0ID09PSAnYXNjZW5kJykge1xuICAgICAgICB0aGlzLnNldFNvcnRWYWx1ZShudWxsKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc2V0U29ydFZhbHVlKCdkZXNjZW5kJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgc2V0U29ydFZhbHVlKHZhbHVlOiAnYXNjZW5kJyB8ICdkZXNjZW5kJyB8IG51bGwpOiB2b2lkIHtcbiAgICB0aGlzLm56U29ydCA9IHZhbHVlO1xuICAgIHRoaXMubnpTb3J0Q2hhbmdlV2l0aEtleS5lbWl0KHsga2V5OiB0aGlzLm56U29ydEtleSwgdmFsdWU6IHRoaXMubnpTb3J0IH0pO1xuICAgIHRoaXMubnpTb3J0Q2hhbmdlLmVtaXQodGhpcy5uelNvcnQpO1xuICB9XG5cbiAgZ2V0IGZpbHRlckxpc3QoKTogTnpUaEl0ZW1JbnRlcmZhY2VbXSB7XG4gICAgcmV0dXJuIHRoaXMubXVsdGlwbGVGaWx0ZXJMaXN0LmZpbHRlcihpdGVtID0+IGl0ZW0uY2hlY2tlZCkubWFwKGl0ZW0gPT4gaXRlbS52YWx1ZSk7XG4gIH1cblxuICAvKiB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55ICovXG4gIGdldCBmaWx0ZXJWYWx1ZSgpOiBhbnkge1xuICAgIGNvbnN0IGNoZWNrZWRGaWx0ZXIgPSB0aGlzLnNpbmdsZUZpbHRlckxpc3QuZmluZChpdGVtID0+IGl0ZW0uY2hlY2tlZCk7XG4gICAgcmV0dXJuIGNoZWNrZWRGaWx0ZXIgPyBjaGVja2VkRmlsdGVyLnZhbHVlIDogbnVsbDtcbiAgfVxuXG4gIHVwZGF0ZUZpbHRlclN0YXR1cygpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uekZpbHRlck11bHRpcGxlKSB7XG4gICAgICB0aGlzLmhhc0ZpbHRlclZhbHVlID0gdGhpcy5maWx0ZXJMaXN0Lmxlbmd0aCA+IDA7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuaGFzRmlsdGVyVmFsdWUgPSBpc05vdE5pbCh0aGlzLmZpbHRlclZhbHVlKTtcbiAgICB9XG4gIH1cblxuICBzZWFyY2goKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVGaWx0ZXJTdGF0dXMoKTtcbiAgICBpZiAodGhpcy5uekZpbHRlck11bHRpcGxlKSB7XG4gICAgICB0aGlzLm56RmlsdGVyQ2hhbmdlLmVtaXQodGhpcy5maWx0ZXJMaXN0KTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5uekZpbHRlckNoYW5nZS5lbWl0KHRoaXMuZmlsdGVyVmFsdWUpO1xuICAgIH1cbiAgfVxuXG4gIHJlc2V0KCk6IHZvaWQge1xuICAgIHRoaXMuaW5pdE11bHRpcGxlRmlsdGVyTGlzdCh0cnVlKTtcbiAgICB0aGlzLmluaXRTaW5nbGVGaWx0ZXJMaXN0KHRydWUpO1xuICAgIHRoaXMuaGFzRmlsdGVyVmFsdWUgPSBmYWxzZTtcbiAgfVxuXG4gIGNoZWNrTXVsdGlwbGUoZmlsdGVyOiBOelRoSXRlbUludGVyZmFjZSk6IHZvaWQge1xuICAgIGZpbHRlci5jaGVja2VkID0gIWZpbHRlci5jaGVja2VkO1xuICB9XG5cbiAgY2hlY2tTaW5nbGUoZmlsdGVyOiBOelRoSXRlbUludGVyZmFjZSk6IHZvaWQge1xuICAgIHRoaXMuc2luZ2xlRmlsdGVyTGlzdC5mb3JFYWNoKGl0ZW0gPT4gKGl0ZW0uY2hlY2tlZCA9IGl0ZW0gPT09IGZpbHRlcikpO1xuICB9XG5cbiAgaGlkZURyb3BEb3duKCk6IHZvaWQge1xuICAgIHRoaXMubnpEcm9wRG93bkNvbXBvbmVudC5zZXRWaXNpYmxlU3RhdGVXaGVuKGZhbHNlKTtcbiAgICB0aGlzLmZpbHRlclZpc2libGUgPSBmYWxzZTtcbiAgfVxuXG4gIGRyb3BEb3duVmlzaWJsZUNoYW5nZSh2YWx1ZTogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMuZmlsdGVyVmlzaWJsZSA9IHZhbHVlO1xuICAgIGlmICghdmFsdWUpIHtcbiAgICAgIHRoaXMuc2VhcmNoKCk7XG4gICAgfVxuICB9XG5cbiAgaW5pdE11bHRpcGxlRmlsdGVyTGlzdChmb3JjZT86IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm11bHRpcGxlRmlsdGVyTGlzdCA9IHRoaXMubnpGaWx0ZXJzLm1hcChpdGVtID0+IHtcbiAgICAgIGNvbnN0IGNoZWNrZWQgPSBmb3JjZSA/IGZhbHNlIDogISFpdGVtLmJ5RGVmYXVsdDtcbiAgICAgIGlmIChjaGVja2VkKSB7XG4gICAgICAgIHRoaXMuaGFzRGVmYXVsdEZpbHRlciA9IHRydWU7XG4gICAgICB9XG4gICAgICByZXR1cm4geyB0ZXh0OiBpdGVtLnRleHQsIHZhbHVlOiBpdGVtLnZhbHVlLCBjaGVja2VkIH07XG4gICAgfSk7XG4gICAgdGhpcy5jaGVja0RlZmF1bHRGaWx0ZXJzKCk7XG4gIH1cblxuICBpbml0U2luZ2xlRmlsdGVyTGlzdChmb3JjZT86IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLnNpbmdsZUZpbHRlckxpc3QgPSB0aGlzLm56RmlsdGVycy5tYXAoaXRlbSA9PiB7XG4gICAgICBjb25zdCBjaGVja2VkID0gZm9yY2UgPyBmYWxzZSA6ICEhaXRlbS5ieURlZmF1bHQ7XG4gICAgICBpZiAoY2hlY2tlZCkge1xuICAgICAgICB0aGlzLmhhc0RlZmF1bHRGaWx0ZXIgPSB0cnVlO1xuICAgICAgfVxuICAgICAgcmV0dXJuIHsgdGV4dDogaXRlbS50ZXh0LCB2YWx1ZTogaXRlbS52YWx1ZSwgY2hlY2tlZCB9O1xuICAgIH0pO1xuICAgIHRoaXMuY2hlY2tEZWZhdWx0RmlsdGVycygpO1xuICB9XG5cbiAgY2hlY2tEZWZhdWx0RmlsdGVycygpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMubnpGaWx0ZXJzIHx8IHRoaXMubnpGaWx0ZXJzLmxlbmd0aCA9PT0gMCB8fCAhdGhpcy5oYXNEZWZhdWx0RmlsdGVyKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMudXBkYXRlRmlsdGVyU3RhdHVzKCk7XG4gIH1cblxuICBtYXJGb3JDaGVjaygpOiB2b2lkIHtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZiwgcHJpdmF0ZSBpMThuOiBOekkxOG5TZXJ2aWNlKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuaTE4bi5sb2NhbGVDaGFuZ2UucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95JCkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLmxvY2FsZSA9IHRoaXMuaTE4bi5nZXRMb2NhbGVEYXRhKCdUYWJsZScpO1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpGaWx0ZXJzKSB7XG4gICAgICB0aGlzLmluaXRNdWx0aXBsZUZpbHRlckxpc3QoKTtcbiAgICAgIHRoaXMuaW5pdFNpbmdsZUZpbHRlckxpc3QoKTtcbiAgICAgIHRoaXMudXBkYXRlRmlsdGVyU3RhdHVzKCk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56V2lkdGgpIHtcbiAgICAgIHRoaXMubnpXaWR0aENoYW5nZSQubmV4dCh0aGlzLm56V2lkdGgpO1xuICAgIH1cbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuZGVzdHJveSQubmV4dCgpO1xuICAgIHRoaXMuZGVzdHJveSQuY29tcGxldGUoKTtcbiAgfVxufVxuIl19