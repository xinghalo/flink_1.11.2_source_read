/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, EventEmitter, Input, Output, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { isInteger } from '../core/util/check';
import { toNumber, InputBoolean, InputNumber } from '../core/util/convert';
import { NzI18nService } from '../i18n/nz-i18n.service';
var NzPaginationComponent = /** @class */ (function () {
    function NzPaginationComponent(i18n, cdr) {
        this.i18n = i18n;
        this.cdr = cdr;
        // tslint:disable-next-line:no-any
        this.locale = {};
        this.firstIndex = 1;
        this.pages = [];
        this.$destroy = new Subject();
        this.nzPageSizeChange = new EventEmitter();
        this.nzPageIndexChange = new EventEmitter();
        this.nzInTable = false;
        this.nzSize = 'default';
        this.nzPageSizeOptions = [10, 20, 30, 40];
        this.nzShowSizeChanger = false;
        this.nzHideOnSinglePage = false;
        this.nzShowQuickJumper = false;
        this.nzSimple = false;
        this.nzTotal = 0;
        this.nzPageIndex = 1;
        this.nzPageSize = 10;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    NzPaginationComponent.prototype.validatePageIndex = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (value > this.lastIndex) {
            return this.lastIndex;
        }
        else if (value < this.firstIndex) {
            return this.firstIndex;
        }
        else {
            return value;
        }
    };
    /**
     * @param {?} page
     * @return {?}
     */
    NzPaginationComponent.prototype.updatePageIndexValue = /**
     * @param {?} page
     * @return {?}
     */
    function (page) {
        this.nzPageIndex = page;
        this.nzPageIndexChange.emit(this.nzPageIndex);
        this.buildIndexes();
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzPaginationComponent.prototype.isPageIndexValid = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        return this.validatePageIndex(value) === value;
    };
    /**
     * @param {?} index
     * @return {?}
     */
    NzPaginationComponent.prototype.jumpPage = /**
     * @param {?} index
     * @return {?}
     */
    function (index) {
        if (index !== this.nzPageIndex) {
            /** @type {?} */
            var pageIndex = this.validatePageIndex(index);
            if (pageIndex !== this.nzPageIndex) {
                this.updatePageIndexValue(pageIndex);
            }
        }
    };
    /**
     * @param {?} diff
     * @return {?}
     */
    NzPaginationComponent.prototype.jumpDiff = /**
     * @param {?} diff
     * @return {?}
     */
    function (diff) {
        this.jumpPage(this.nzPageIndex + diff);
    };
    /**
     * @param {?} $event
     * @return {?}
     */
    NzPaginationComponent.prototype.onPageSizeChange = /**
     * @param {?} $event
     * @return {?}
     */
    function ($event) {
        this.nzPageSize = $event;
        this.nzPageSizeChange.emit($event);
        this.buildIndexes();
        if (this.nzPageIndex > this.lastIndex) {
            this.updatePageIndexValue(this.lastIndex);
        }
    };
    /**
     * @param {?} _
     * @param {?} input
     * @param {?} clearInputValue
     * @return {?}
     */
    NzPaginationComponent.prototype.handleKeyDown = /**
     * @param {?} _
     * @param {?} input
     * @param {?} clearInputValue
     * @return {?}
     */
    function (_, input, clearInputValue) {
        /** @type {?} */
        var target = input;
        /** @type {?} */
        var page = toNumber(target.value, this.nzPageIndex);
        if (isInteger(page) && this.isPageIndexValid(page) && page !== this.nzPageIndex) {
            this.updatePageIndexValue(page);
        }
        if (clearInputValue) {
            target.value = '';
        }
        else {
            target.value = "" + this.nzPageIndex;
        }
    };
    /** generate indexes list */
    /**
     * generate indexes list
     * @return {?}
     */
    NzPaginationComponent.prototype.buildIndexes = /**
     * generate indexes list
     * @return {?}
     */
    function () {
        /** @type {?} */
        var pages = [];
        if (this.lastIndex <= 9) {
            for (var i = 2; i <= this.lastIndex - 1; i++) {
                pages.push(i);
            }
        }
        else {
            /** @type {?} */
            var current = +this.nzPageIndex;
            /** @type {?} */
            var left = Math.max(2, current - 2);
            /** @type {?} */
            var right = Math.min(current + 2, this.lastIndex - 1);
            if (current - 1 <= 2) {
                right = 5;
            }
            if (this.lastIndex - current <= 2) {
                left = this.lastIndex - 4;
            }
            for (var i = left; i <= right; i++) {
                pages.push(i);
            }
        }
        this.pages = pages;
        this.cdr.markForCheck();
    };
    Object.defineProperty(NzPaginationComponent.prototype, "lastIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return Math.ceil(this.nzTotal / this.nzPageSize);
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzPaginationComponent.prototype, "isLastIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzPageIndex === this.lastIndex;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzPaginationComponent.prototype, "isFirstIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzPageIndex === this.firstIndex;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzPaginationComponent.prototype, "ranges", {
        get: /**
         * @return {?}
         */
        function () {
            return [(this.nzPageIndex - 1) * this.nzPageSize + 1, Math.min(this.nzPageIndex * this.nzPageSize, this.nzTotal)];
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzPaginationComponent.prototype, "showAddOption", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzPageSizeOptions.indexOf(this.nzPageSize) === -1;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzPaginationComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.i18n.localeChange.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @return {?}
         */
        function () {
            _this.locale = _this.i18n.getLocaleData('Pagination');
            _this.cdr.markForCheck();
        }));
    };
    /**
     * @return {?}
     */
    NzPaginationComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.$destroy.next();
        this.$destroy.complete();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzPaginationComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzTotal || changes.nzPageSize || changes.nzPageIndex) {
            this.buildIndexes();
        }
    };
    NzPaginationComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-pagination',
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    template: "<ng-template #renderItemTemplate let-type let-page=\"page\">\n  <a class=\"ant-pagination-item-link\" *ngIf=\"type==='pre'\"><i nz-icon type=\"left\"></i></a>\n  <a class=\"ant-pagination-item-link\" *ngIf=\"type==='next'\"><i nz-icon type=\"right\"></i></a>\n  <a *ngIf=\"type=='page'\">{{ page }}</a>\n</ng-template>\n<ng-container *ngIf=\"nzHideOnSinglePage && (nzTotal > nzPageSize) || !nzHideOnSinglePage\">\n  <ul class=\"ant-pagination\"\n    [class.ant-table-pagination]=\"nzInTable\"\n    [class.ant-pagination-simple]=\"nzSimple\"\n    [class.mini]=\"(nzSize === 'small') && !nzSimple\">\n    <ng-container *ngIf=\"nzSimple; else normalTemplate\">\n      <li class=\"ant-pagination-prev\"\n        [attr.title]=\"locale.prev_page\"\n        [class.ant-pagination-disabled]=\"isFirstIndex\"\n        (click)=\"jumpDiff(-1)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'pre'}\"></ng-template>\n      </li>\n      <li [attr.title]=\"nzPageIndex+'/'+lastIndex\" class=\"ant-pagination-simple-pager\">\n        <input #simplePagerInput [value]=\"nzPageIndex\" (keydown.enter)=\"handleKeyDown($event,simplePagerInput,false)\" size=\"3\">\n        <span class=\"ant-pagination-slash\">\uFF0F</span>\n        {{ lastIndex }}\n      </li>\n      <li class=\"ant-pagination-next\"\n        [attr.title]=\"locale.next_page\"\n        [class.ant-pagination-disabled]=\"isLastIndex\"\n        (click)=\"jumpDiff(1)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'next'}\"></ng-template>\n      </li>\n    </ng-container>\n    <ng-template #normalTemplate>\n      <li class=\"ant-pagination-total-text\" *ngIf=\"nzShowTotal\">\n        <ng-template [ngTemplateOutlet]=\"nzShowTotal\" [ngTemplateOutletContext]=\"{ $implicit: nzTotal,range:ranges }\"></ng-template>\n      </li>\n      <li class=\"ant-pagination-prev\"\n        [attr.title]=\"locale.prev_page\"\n        [class.ant-pagination-disabled]=\"isFirstIndex\"\n        (click)=\"jumpDiff(-1)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'pre'}\"></ng-template>\n      </li>\n      <li class=\"ant-pagination-item\"\n        [attr.title]=\"firstIndex\"\n        [class.ant-pagination-item-active]=\"isFirstIndex\"\n        (click)=\"jumpPage(firstIndex)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'page',page: firstIndex }\"></ng-template>\n      </li>\n      <li class=\"ant-pagination-jump-prev\"\n        *ngIf=\"(lastIndex > 9) && (nzPageIndex - 3 > firstIndex)\"\n        [attr.title]=\"locale.prev_5\"\n        (click)=\"jumpDiff(-5)\">\n        <a class=\"ant-pagination-item-link\">\n          <div class=\"ant-pagination-item-container\">\n            <i nz-icon type=\"double-left\" class=\"ant-pagination-item-link-icon\"></i>\n            <span class=\"ant-pagination-item-ellipsis\">\u2022\u2022\u2022</span>\n          </div>\n        </a>\n      </li>\n      <li class=\"ant-pagination-item\"\n        *ngFor=\"let page of pages\"\n        [attr.title]=\"page\"\n        [class.ant-pagination-item-active]=\"nzPageIndex === page\"\n        (click)=\"jumpPage(page)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'page',page: page }\"></ng-template>\n      </li>\n      <li class=\"ant-pagination-jump-next ant-pagination-item-link-icon\"\n        [attr.title]=\"locale.next_5\"\n        (click)=\"jumpDiff(5)\"\n        *ngIf=\"(lastIndex > 9) && (nzPageIndex + 3 < lastIndex)\">\n        <a class=\"ant-pagination-item-link\">\n          <div class=\"ant-pagination-item-container\">\n            <i nz-icon type=\"double-right\" class=\"ant-pagination-item-link-icon\"></i>\n            <span class=\"ant-pagination-item-ellipsis\">\u2022\u2022\u2022</span>\n          </div>\n        </a>\n      </li>\n      <li class=\"ant-pagination-item\"\n        [attr.title]=\"lastIndex\"\n        (click)=\"jumpPage(lastIndex)\"\n        *ngIf=\"(lastIndex > 0) && (lastIndex !== firstIndex)\"\n        [class.ant-pagination-item-active]=\"isLastIndex\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'page',page: lastIndex }\"></ng-template>\n      </li>\n      <li class=\"ant-pagination-next\"\n        [title]=\"locale.next_page\"\n        [class.ant-pagination-disabled]=\"isLastIndex\"\n        (click)=\"jumpDiff(1)\">\n        <ng-template [ngTemplateOutlet]=\"nzItemRender\" [ngTemplateOutletContext]=\"{ $implicit: 'next'}\"></ng-template>\n      </li>\n      <div class=\"ant-pagination-options\" *ngIf=\"nzShowQuickJumper || nzShowSizeChanger\">\n        <nz-select class=\"ant-pagination-options-size-changer\"\n          *ngIf=\"nzShowSizeChanger\"\n          [nzSize]=\"nzSize\"\n          [ngModel]=\"nzPageSize\"\n          (ngModelChange)=\"onPageSizeChange($event)\">\n          <nz-option *ngFor=\"let option of nzPageSizeOptions\"\n            [nzLabel]=\"option + locale.items_per_page\"\n            [nzValue]=\"option\">\n          </nz-option>\n          <nz-option *ngIf=\"showAddOption\"\n            [nzLabel]=\"nzPageSize + locale.items_per_page\"\n            [nzValue]=\"nzPageSize\">\n          </nz-option>\n        </nz-select>\n        <div class=\"ant-pagination-options-quick-jumper\" *ngIf=\"nzShowQuickJumper\">\n          {{ locale.jump_to }}\n          <input #quickJumperInput (keydown.enter)=\"handleKeyDown($event,quickJumperInput,true)\">\n          {{ locale.page }}\n        </div>\n      </div>\n    </ng-template>\n  </ul>\n</ng-container>"
                }] }
    ];
    /** @nocollapse */
    NzPaginationComponent.ctorParameters = function () { return [
        { type: NzI18nService },
        { type: ChangeDetectorRef }
    ]; };
    NzPaginationComponent.propDecorators = {
        nzPageSizeChange: [{ type: Output }],
        nzPageIndexChange: [{ type: Output }],
        nzShowTotal: [{ type: Input }],
        nzInTable: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzPageSizeOptions: [{ type: Input }],
        nzItemRender: [{ type: Input }, { type: ViewChild, args: ['renderItemTemplate',] }],
        nzShowSizeChanger: [{ type: Input }],
        nzHideOnSinglePage: [{ type: Input }],
        nzShowQuickJumper: [{ type: Input }],
        nzSimple: [{ type: Input }],
        nzTotal: [{ type: Input }],
        nzPageIndex: [{ type: Input }],
        nzPageSize: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzShowSizeChanger", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzHideOnSinglePage", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzShowQuickJumper", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzSimple", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzTotal", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzPageIndex", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzPaginationComponent.prototype, "nzPageSize", void 0);
    return NzPaginationComponent;
}());
export { NzPaginationComponent };
if (false) {
    /** @type {?} */
    NzPaginationComponent.prototype.locale;
    /** @type {?} */
    NzPaginationComponent.prototype.firstIndex;
    /** @type {?} */
    NzPaginationComponent.prototype.pages;
    /**
     * @type {?}
     * @private
     */
    NzPaginationComponent.prototype.$destroy;
    /** @type {?} */
    NzPaginationComponent.prototype.nzPageSizeChange;
    /** @type {?} */
    NzPaginationComponent.prototype.nzPageIndexChange;
    /** @type {?} */
    NzPaginationComponent.prototype.nzShowTotal;
    /** @type {?} */
    NzPaginationComponent.prototype.nzInTable;
    /** @type {?} */
    NzPaginationComponent.prototype.nzSize;
    /** @type {?} */
    NzPaginationComponent.prototype.nzPageSizeOptions;
    /** @type {?} */
    NzPaginationComponent.prototype.nzItemRender;
    /** @type {?} */
    NzPaginationComponent.prototype.nzShowSizeChanger;
    /** @type {?} */
    NzPaginationComponent.prototype.nzHideOnSinglePage;
    /** @type {?} */
    NzPaginationComponent.prototype.nzShowQuickJumper;
    /** @type {?} */
    NzPaginationComponent.prototype.nzSimple;
    /** @type {?} */
    NzPaginationComponent.prototype.nzTotal;
    /** @type {?} */
    NzPaginationComponent.prototype.nzPageIndex;
    /** @type {?} */
    NzPaginationComponent.prototype.nzPageSize;
    /**
     * @type {?}
     * @private
     */
    NzPaginationComponent.prototype.i18n;
    /**
     * @type {?}
     * @private
     */
    NzPaginationComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcGFnaW5hdGlvbi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicGFnaW5hdGlvbi9uei1wYWdpbmF0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUlMLE1BQU0sRUFFTixXQUFXLEVBQ1gsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUMzQyxPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFDL0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDM0UsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBRXhEO0lBbUlFLCtCQUFvQixJQUFtQixFQUFVLEdBQXNCO1FBQW5ELFNBQUksR0FBSixJQUFJLENBQWU7UUFBVSxRQUFHLEdBQUgsR0FBRyxDQUFtQjs7UUExSHZFLFdBQU0sR0FBUSxFQUFFLENBQUM7UUFDakIsZUFBVSxHQUFHLENBQUMsQ0FBQztRQUNmLFVBQUssR0FBYSxFQUFFLENBQUM7UUFDYixhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUNwQixxQkFBZ0IsR0FBeUIsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUM1RCxzQkFBaUIsR0FBeUIsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUV2RSxjQUFTLEdBQUcsS0FBSyxDQUFDO1FBQ2xCLFdBQU0sR0FBd0IsU0FBUyxDQUFDO1FBQ3hDLHNCQUFpQixHQUFHLENBQUMsRUFBRSxFQUFFLEVBQUUsRUFB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QWtGOztJQUNsRiwwQ0FBMkI7O0lBQzNCLHVDQUFpRDs7SUFDakQsa0RBQThDOztJQUM5Qyw2Q0FHRzs7SUFDSCxrREFBbUQ7O0lBQ25ELG1EQUFvRDs7SUFDcEQsa0RBQW1EOztJQUNuRCx5Q0FBMEM7O0lBQzFDLHdDQUFvQzs7SUFDcEMsNENBQXdDOztJQUN4QywyQ0FBd0M7Ozs7O0lBc0c1QixxQ0FBMkI7Ozs7O0lBQUUsb0NBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBpc0ludGVnZXIgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuaW1wb3J0IHsgdG9OdW1iZXIsIElucHV0Qm9vbGVhbiwgSW5wdXROdW1iZXIgfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBOekkxOG5TZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1wYWdpbmF0aW9uJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotcGFnaW5hdGlvbi5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpQYWdpbmF0aW9uQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkRlc3Ryb3ksIE9uQ2hhbmdlcyB7XG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgbG9jYWxlOiBhbnkgPSB7fTtcbiAgZmlyc3RJbmRleCA9IDE7XG4gIHBhZ2VzOiBudW1iZXJbXSA9IFtdO1xuICBwcml2YXRlICRkZXN0cm95ID0gbmV3IFN1YmplY3Q8dm9pZD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56UGFnZVNpemVDaGFuZ2U6IEV2ZW50RW1pdHRlcjxudW1iZXI+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpQYWdlSW5kZXhDaGFuZ2U6IEV2ZW50RW1pdHRlcjxudW1iZXI+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuICBASW5wdXQoKSBuelNob3dUb3RhbDogVGVtcGxhdGVSZWY8eyAkaW1wbGljaXQ6IG51bWJlcjsgcmFuZ2U6IFtudW1iZXIsIG51bWJlcl0gfT47XG4gIEBJbnB1dCgpIG56SW5UYWJsZSA9IGZhbHNlO1xuICBASW5wdXQoKSBuelNpemU6ICdkZWZhdWx0JyB8ICdzbWFsbCcgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56UGFnZVNpemVPcHRpb25zID0gWzEwLCAyMCwgMzAsIDQwXTtcbiAgQElucHV0KCkgQFZpZXdDaGlsZCgncmVuZGVySXRlbVRlbXBsYXRlJykgbnpJdGVtUmVuZGVyOiBUZW1wbGF0ZVJlZjx7XG4gICAgJGltcGxpY2l0OiAncGFnZScgfCAncHJldicgfCAnbmV4dCc7XG4gICAgcGFnZTogbnVtYmVyO1xuICB9PjtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1NpemVDaGFuZ2VyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekhpZGVPblNpbmdsZVBhZ2UgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1F1aWNrSnVtcGVyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNpbXBsZSA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuelRvdGFsID0gMDtcbiAgQElucHV0KCkgQElucHV0TnVtYmVyKCkgbnpQYWdlSW5kZXggPSAxO1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuelBhZ2VTaXplID0gMTA7XG5cbiAgdmFsaWRhdGVQYWdlSW5kZXgodmFsdWU6IG51bWJlcik6IG51bWJlciB7XG4gICAgaWYgKHZhbHVlID4gdGhpcy5sYXN0SW5kZXgpIHtcbiAgICAgIHJldHVybiB0aGlzLmxhc3RJbmRleDtcbiAgICB9IGVsc2UgaWYgKHZhbHVlIDwgdGhpcy5maXJzdEluZGV4KSB7XG4gICAgICByZXR1cm4gdGhpcy5maXJzdEluZGV4O1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgdXBkYXRlUGFnZUluZGV4VmFsdWUocGFnZTogbnVtYmVyKTogdm9pZCB7XG4gICAgdGhpcy5uelBhZ2VJbmRleCA9IHBhZ2U7XG4gICAgdGhpcy5uelBhZ2VJbmRleENoYW5nZS5lbWl0KHRoaXMubnpQYWdlSW5kZXgpO1xuICAgIHRoaXMuYnVpbGRJbmRleGVzKCk7XG4gIH1cblxuICBpc1BhZ2VJbmRleFZhbGlkKHZhbHVlOiBudW1iZXIpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy52YWxpZGF0ZVBhZ2VJbmRleCh2YWx1ZSkgPT09IHZhbHVlO1xuICB9XG5cbiAganVtcFBhZ2UoaW5kZXg6IG51bWJlcik6IHZvaWQge1xuICAgIGlmIChpbmRleCAhPT0gdGhpcy5uelBhZ2VJbmRleCkge1xuICAgICAgY29uc3QgcGFnZUluZGV4ID0gdGhpcy52YWxpZGF0ZVBhZ2VJbmRleChpbmRleCk7XG4gICAgICBpZiAocGFnZUluZGV4ICE9PSB0aGlzLm56UGFnZUluZGV4KSB7XG4gICAgICAgIHRoaXMudXBkYXRlUGFnZUluZGV4VmFsdWUocGFnZUluZGV4KTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBqdW1wRGlmZihkaWZmOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLmp1bXBQYWdlKHRoaXMubnpQYWdlSW5kZXggKyBkaWZmKTtcbiAgfVxuXG4gIG9uUGFnZVNpemVDaGFuZ2UoJGV2ZW50OiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLm56UGFnZVNpemUgPSAkZXZlbnQ7XG4gICAgdGhpcy5uelBhZ2VTaXplQ2hhbmdlLmVtaXQoJGV2ZW50KTtcbiAgICB0aGlzLmJ1aWxkSW5kZXhlcygpO1xuICAgIGlmICh0aGlzLm56UGFnZUluZGV4ID4gdGhpcy5sYXN0SW5kZXgpIHtcbiAgICAgIHRoaXMudXBkYXRlUGFnZUluZGV4VmFsdWUodGhpcy5sYXN0SW5kZXgpO1xuICAgIH1cbiAgfVxuXG4gIGhhbmRsZUtleURvd24oXzogS2V5Ym9hcmRFdmVudCwgaW5wdXQ6IEhUTUxJbnB1dEVsZW1lbnQsIGNsZWFySW5wdXRWYWx1ZTogYm9vbGVhbik6IHZvaWQge1xuICAgIGNvbnN0IHRhcmdldCA9IGlucHV0O1xuICAgIGNvbnN0IHBhZ2UgPSB0b051bWJlcih0YXJnZXQudmFsdWUsIHRoaXMubnpQYWdlSW5kZXgpO1xuICAgIGlmIChpc0ludGVnZXIocGFnZSkgJiYgdGhpcy5pc1BhZ2VJbmRleFZhbGlkKHBhZ2UpICYmIHBhZ2UgIT09IHRoaXMubnpQYWdlSW5kZXgpIHtcbiAgICAgIHRoaXMudXBkYXRlUGFnZUluZGV4VmFsdWUocGFnZSk7XG4gICAgfVxuICAgIGlmIChjbGVhcklucHV0VmFsdWUpIHtcbiAgICAgIHRhcmdldC52YWx1ZSA9ICcnO1xuICAgIH0gZWxzZSB7XG4gICAgICB0YXJnZXQudmFsdWUgPSBgJHt0aGlzLm56UGFnZUluZGV4fWA7XG4gICAgfVxuICB9XG5cbiAgLyoqIGdlbmVyYXRlIGluZGV4ZXMgbGlzdCAqL1xuICBidWlsZEluZGV4ZXMoKTogdm9pZCB7XG4gICAgY29uc3QgcGFnZXM6IG51bWJlcltdID0gW107XG4gICAgaWYgKHRoaXMubGFzdEluZGV4IDw9IDkpIHtcbiAgICAgIGZvciAobGV0IGkgPSAyOyBpIDw9IHRoaXMubGFzdEluZGV4IC0gMTsgaSsrKSB7XG4gICAgICAgIHBhZ2VzLnB1c2goaSk7XG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIGNvbnN0IGN1cnJlbnQgPSArdGhpcy5uelBhZ2VJbmRleDtcbiAgICAgIGxldCBsZWZ0ID0gTWF0aC5tYXgoMiwgY3VycmVudCAtIDIpO1xuICAgICAgbGV0IHJpZ2h0ID0gTWF0aC5taW4oY3VycmVudCArIDIsIHRoaXMubGFzdEluZGV4IC0gMSk7XG4gICAgICBpZiAoY3VycmVudCAtIDEgPD0gMikge1xuICAgICAgICByaWdodCA9IDU7XG4gICAgICB9XG4gICAgICBpZiAodGhpcy5sYXN0SW5kZXggLSBjdXJyZW50IDw9IDIpIHtcbiAgICAgICAgbGVmdCA9IHRoaXMubGFzdEluZGV4IC0gNDtcbiAgICAgIH1cbiAgICAgIGZvciAobGV0IGkgPSBsZWZ0OyBpIDw9IHJpZ2h0OyBpKyspIHtcbiAgICAgICAgcGFnZXMucHVzaChpKTtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5wYWdlcyA9IHBhZ2VzO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgZ2V0IGxhc3RJbmRleCgpOiBudW1iZXIge1xuICAgIHJldHVybiBNYXRoLmNlaWwodGhpcy5uelRvdGFsIC8gdGhpcy5uelBhZ2VTaXplKTtcbiAgfVxuXG4gIGdldCBpc0xhc3RJbmRleCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5uelBhZ2VJbmRleCA9PT0gdGhpcy5sYXN0SW5kZXg7XG4gIH1cblxuICBnZXQgaXNGaXJzdEluZGV4KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm56UGFnZUluZGV4ID09PSB0aGlzLmZpcnN0SW5kZXg7XG4gIH1cblxuICBnZXQgcmFuZ2VzKCk6IG51bWJlcltdIHtcbiAgICByZXR1cm4gWyh0aGlzLm56UGFnZUluZGV4IC0gMSkgKiB0aGlzLm56UGFnZVNpemUgKyAxLCBNYXRoLm1pbih0aGlzLm56UGFnZUluZGV4ICogdGhpcy5uelBhZ2VTaXplLCB0aGlzLm56VG90YWwpXTtcbiAgfVxuXG4gIGdldCBzaG93QWRkT3B0aW9uKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm56UGFnZVNpemVPcHRpb25zLmluZGV4T2YodGhpcy5uelBhZ2VTaXplKSA9PT0gLTE7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGkxOG46IE56STE4blNlcnZpY2UsIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZikge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmkxOG4ubG9jYWxlQ2hhbmdlLnBpcGUodGFrZVVudGlsKHRoaXMuJGRlc3Ryb3kpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgdGhpcy5sb2NhbGUgPSB0aGlzLmkxOG4uZ2V0TG9jYWxlRGF0YSgnUGFnaW5hdGlvbicpO1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLiRkZXN0cm95Lm5leHQoKTtcbiAgICB0aGlzLiRkZXN0cm95LmNvbXBsZXRlKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpUb3RhbCB8fCBjaGFuZ2VzLm56UGFnZVNpemUgfHwgY2hhbmdlcy5uelBhZ2VJbmRleCkge1xuICAgICAgdGhpcy5idWlsZEluZGV4ZXMoKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==