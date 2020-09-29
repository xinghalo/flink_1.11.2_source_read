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
export class NzPaginationComponent {
    /**
     * @param {?} i18n
     * @param {?} cdr
     */
    constructor(i18n, cdr) {
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
    validatePageIndex(value) {
        if (value > this.lastIndex) {
            return this.lastIndex;
        }
        else if (value < this.firstIndex) {
            return this.firstIndex;
        }
        else {
            return value;
        }
    }
    /**
     * @param {?} page
     * @return {?}
     */
    updatePageIndexValue(page) {
        this.nzPageIndex = page;
        this.nzPageIndexChange.emit(this.nzPageIndex);
        this.buildIndexes();
    }
    /**
     * @param {?} value
     * @return {?}
     */
    isPageIndexValid(value) {
        return this.validatePageIndex(value) === value;
    }
    /**
     * @param {?} index
     * @return {?}
     */
    jumpPage(index) {
        if (index !== this.nzPageIndex) {
            /** @type {?} */
            const pageIndex = this.validatePageIndex(index);
            if (pageIndex !== this.nzPageIndex) {
                this.updatePageIndexValue(pageIndex);
            }
        }
    }
    /**
     * @param {?} diff
     * @return {?}
     */
    jumpDiff(diff) {
        this.jumpPage(this.nzPageIndex + diff);
    }
    /**
     * @param {?} $event
     * @return {?}
     */
    onPageSizeChange($event) {
        this.nzPageSize = $event;
        this.nzPageSizeChange.emit($event);
        this.buildIndexes();
        if (this.nzPageIndex > this.lastIndex) {
            this.updatePageIndexValue(this.lastIndex);
        }
    }
    /**
     * @param {?} _
     * @param {?} input
     * @param {?} clearInputValue
     * @return {?}
     */
    handleKeyDown(_, input, clearInputValue) {
        /** @type {?} */
        const target = input;
        /** @type {?} */
        const page = toNumber(target.value, this.nzPageIndex);
        if (isInteger(page) && this.isPageIndexValid(page) && page !== this.nzPageIndex) {
            this.updatePageIndexValue(page);
        }
        if (clearInputValue) {
            target.value = '';
        }
        else {
            target.value = `${this.nzPageIndex}`;
        }
    }
    /**
     * generate indexes list
     * @return {?}
     */
    buildIndexes() {
        /** @type {?} */
        const pages = [];
        if (this.lastIndex <= 9) {
            for (let i = 2; i <= this.lastIndex - 1; i++) {
                pages.push(i);
            }
        }
        else {
            /** @type {?} */
            const current = +this.nzPageIndex;
            /** @type {?} */
            let left = Math.max(2, current - 2);
            /** @type {?} */
            let right = Math.min(current + 2, this.lastIndex - 1);
            if (current - 1 <= 2) {
                right = 5;
            }
            if (this.lastIndex - current <= 2) {
                left = this.lastIndex - 4;
            }
            for (let i = left; i <= right; i++) {
                pages.push(i);
            }
        }
        this.pages = pages;
        this.cdr.markForCheck();
    }
    /**
     * @return {?}
     */
    get lastIndex() {
        return Math.ceil(this.nzTotal / this.nzPageSize);
    }
    /**
     * @return {?}
     */
    get isLastIndex() {
        return this.nzPageIndex === this.lastIndex;
    }
    /**
     * @return {?}
     */
    get isFirstIndex() {
        return this.nzPageIndex === this.firstIndex;
    }
    /**
     * @return {?}
     */
    get ranges() {
        return [(this.nzPageIndex - 1) * this.nzPageSize + 1, Math.min(this.nzPageIndex * this.nzPageSize, this.nzTotal)];
    }
    /**
     * @return {?}
     */
    get showAddOption() {
        return this.nzPageSizeOptions.indexOf(this.nzPageSize) === -1;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.i18n.localeChange.pipe(takeUntil(this.$destroy)).subscribe((/**
         * @return {?}
         */
        () => {
            this.locale = this.i18n.getLocaleData('Pagination');
            this.cdr.markForCheck();
        }));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.$destroy.next();
        this.$destroy.complete();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzTotal || changes.nzPageSize || changes.nzPageIndex) {
            this.buildIndexes();
        }
    }
}
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
NzPaginationComponent.ctorParameters = () => [
    { type: NzI18nService },
    { type: ChangeDetectorRef }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcGFnaW5hdGlvbi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicGFnaW5hdGlvbi9uei1wYWdpbmF0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osS0FBSyxFQUlMLE1BQU0sRUFFTixXQUFXLEVBQ1gsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQy9CLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUMzQyxPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFDL0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDM0UsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBU3hELE1BQU0sT0FBTyxxQkFBcUI7Ozs7O0lBNEhoQyxZQUFvQixJQUFtQixFQUFVLEdBQXNCO1FBQW5ELFNBQUksR0FBSixJQUFJLENBQWU7UUFBVSxRQUFHLEdBQUgsR0FBRyxDQUFtQjs7UUExSHZFLFdBQU0sR0FBUSxFQUFFLENBQUM7UUFDakIsZUFBVSxHQUFHLENBQUMsQ0FBQztRQUNmLFVBQUssR0FBYSxFQUFFLENBQUM7UUFDYixhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQVEsQ0FBQztRQUNwQixxQkFBZ0IsR0FBeUIsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUM1RCxzQkFBaUIsR0FBeUIsSUFBSSxZQUFZLEVBQUUsQ0FBQztRQUV2RSxjQUFTLEdBQUcsS0FBSyxDQUFDO1FBQ2xCLFdBQU0sR0FBd0IsU0FBUyxDQUFDO1FBQ3hDLHNCQUFpQixHQUFHLENB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WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IHRha2VVbnRpbCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcbmltcG9ydCB7IGlzSW50ZWdlciB9IGZyb20gJy4uL2NvcmUvdXRpbC9jaGVjayc7XG5pbXBvcnQgeyB0b051bWJlciwgSW5wdXRCb29sZWFuLCBJbnB1dE51bWJlciB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56STE4blNlcnZpY2UgfSBmcm9tICcuLi9pMThuL256LWkxOG4uc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LXBhZ2luYXRpb24nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1wYWdpbmF0aW9uLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOelBhZ2luYXRpb25Db21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uRGVzdHJveSwgT25DaGFuZ2VzIHtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBsb2NhbGU6IGFueSA9IHt9O1xuICBmaXJzdEluZGV4ID0gMTtcbiAgcGFnZXM6IG51bWJlcltdID0gW107XG4gIHByaXZhdGUgJGRlc3Ryb3kgPSBuZXcgU3ViamVjdDx2b2lkPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpQYWdlU2l6ZUNoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelBhZ2VJbmRleENoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBJbnB1dCgpIG56U2hvd1RvdGFsOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogbnVtYmVyOyByYW5nZTogW251bWJlciwgbnVtYmVyXSB9PjtcbiAgQElucHV0KCkgbnpJblRhYmxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56U2l6ZTogJ2RlZmF1bHQnIHwgJ3NtYWxsJyA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpQYWdlU2l6ZU9wdGlvbnMgPSBbMTAsIDIwLCAzMCwgNDBdO1xuICBASW5wdXQoKSBAVmlld0NoaWxkKCdyZW5kZXJJdGVtVGVtcGxhdGUnKSBuekl0ZW1SZW5kZXI6IFRlbXBsYXRlUmVmPHtcbiAgICAkaW1wbGljaXQ6ICdwYWdlJyB8ICdwcmV2JyB8ICduZXh0JztcbiAgICBwYWdlOiBudW1iZXI7XG4gIH0+O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93U2l6ZUNoYW5nZXIgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56SGlkZU9uU2luZ2xlUGFnZSA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93UXVpY2tKdW1wZXIgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2ltcGxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56VG90YWwgPSAwO1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuelBhZ2VJbmRleCA9IDE7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56UGFnZVNpemUgPSAxMDtcblxuICB2YWxpZGF0ZVBhZ2VJbmRleCh2YWx1ZTogbnVtYmVyKTogbnVtYmVyIHtcbiAgICBpZiAodmFsdWUgPiB0aGlzLmxhc3RJbmRleCkge1xuICAgICAgcmV0dXJuIHRoaXMubGFzdEluZGV4O1xuICAgIH0gZWxzZSBpZiAodmFsdWUgPCB0aGlzLmZpcnN0SW5kZXgpIHtcbiAgICAgIHJldHVybiB0aGlzLmZpcnN0SW5kZXg7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiB2YWx1ZTtcbiAgICB9XG4gIH1cblxuICB1cGRhdGVQYWdlSW5kZXhWYWx1ZShwYWdlOiBudW1iZXIpOiB2b2lkIHtcbiAgICB0aGlzLm56UGFnZUluZGV4ID0gcGFnZTtcbiAgICB0aGlzLm56UGFnZUluZGV4Q2hhbmdlLmVtaXQodGhpcy5uelBhZ2VJbmRleCk7XG4gICAgdGhpcy5idWlsZEluZGV4ZXMoKTtcbiAgfVxuXG4gIGlzUGFnZUluZGV4VmFsaWQodmFsdWU6IG51bWJlcik6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLnZhbGlkYXRlUGFnZUluZGV4KHZhbHVlKSA9PT0gdmFsdWU7XG4gIH1cblxuICBqdW1wUGFnZShpbmRleDogbnVtYmVyKTogdm9pZCB7XG4gICAgaWYgKGluZGV4ICE9PSB0aGlzLm56UGFnZUluZGV4KSB7XG4gICAgICBjb25zdCBwYWdlSW5kZXggPSB0aGlzLnZhbGlkYXRlUGFnZUluZGV4KGluZGV4KTtcbiAgICAgIGlmIChwYWdlSW5kZXggIT09IHRoaXMubnpQYWdlSW5kZXgpIHtcbiAgICAgICAgdGhpcy51cGRhdGVQYWdlSW5kZXhWYWx1ZShwYWdlSW5kZXgpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIGp1bXBEaWZmKGRpZmY6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMuanVtcFBhZ2UodGhpcy5uelBhZ2VJbmRleCArIGRpZmYpO1xuICB9XG5cbiAgb25QYWdlU2l6ZUNoYW5nZSgkZXZlbnQ6IG51bWJlcik6IHZvaWQge1xuICAgIHRoaXMubnpQYWdlU2l6ZSA9ICRldmVudDtcbiAgICB0aGlzLm56UGFnZVNpemVDaGFuZ2UuZW1pdCgkZXZlbnQpO1xuICAgIHRoaXMuYnVpbGRJbmRleGVzKCk7XG4gICAgaWYgKHRoaXMubnpQYWdlSW5kZXggPiB0aGlzLmxhc3RJbmRleCkge1xuICAgICAgdGhpcy51cGRhdGVQYWdlSW5kZXhWYWx1ZSh0aGlzLmxhc3RJbmRleCk7XG4gICAgfVxuICB9XG5cbiAgaGFuZGxlS2V5RG93bihfOiBLZXlib2FyZEV2ZW50LCBpbnB1dDogSFRNTElucHV0RWxlbWVudCwgY2xlYXJJbnB1dFZhbHVlOiBib29sZWFuKTogdm9pZCB7XG4gICAgY29uc3QgdGFyZ2V0ID0gaW5wdXQ7XG4gICAgY29uc3QgcGFnZSA9IHRvTnVtYmVyKHRhcmdldC52YWx1ZSwgdGhpcy5uelBhZ2VJbmRleCk7XG4gICAgaWYgKGlzSW50ZWdlcihwYWdlKSAmJiB0aGlzLmlzUGFnZUluZGV4VmFsaWQocGFnZSkgJiYgcGFnZSAhPT0gdGhpcy5uelBhZ2VJbmRleCkge1xuICAgICAgdGhpcy51cGRhdGVQYWdlSW5kZXhWYWx1ZShwYWdlKTtcbiAgICB9XG4gICAgaWYgKGNsZWFySW5wdXRWYWx1ZSkge1xuICAgICAgdGFyZ2V0LnZhbHVlID0gJyc7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRhcmdldC52YWx1ZSA9IGAke3RoaXMubnpQYWdlSW5kZXh9YDtcbiAgICB9XG4gIH1cblxuICAvKiogZ2VuZXJhdGUgaW5kZXhlcyBsaXN0ICovXG4gIGJ1aWxkSW5kZXhlcygpOiB2b2lkIHtcbiAgICBjb25zdCBwYWdlczogbnVtYmVyW10gPSBbXTtcbiAgICBpZiAodGhpcy5sYXN0SW5kZXggPD0gOSkge1xuICAgICAgZm9yIChsZXQgaSA9IDI7IGkgPD0gdGhpcy5sYXN0SW5kZXggLSAxOyBpKyspIHtcbiAgICAgICAgcGFnZXMucHVzaChpKTtcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgY29uc3QgY3VycmVudCA9ICt0aGlzLm56UGFnZUluZGV4O1xuICAgICAgbGV0IGxlZnQgPSBNYXRoLm1heCgyLCBjdXJyZW50IC0gMik7XG4gICAgICBsZXQgcmlnaHQgPSBNYXRoLm1pbihjdXJyZW50ICsgMiwgdGhpcy5sYXN0SW5kZXggLSAxKTtcbiAgICAgIGlmIChjdXJyZW50IC0gMSA8PSAyKSB7XG4gICAgICAgIHJpZ2h0ID0gNTtcbiAgICAgIH1cbiAgICAgIGlmICh0aGlzLmxhc3RJbmRleCAtIGN1cnJlbnQgPD0gMikge1xuICAgICAgICBsZWZ0ID0gdGhpcy5sYXN0SW5kZXggLSA0O1xuICAgICAgfVxuICAgICAgZm9yIChsZXQgaSA9IGxlZnQ7IGkgPD0gcmlnaHQ7IGkrKykge1xuICAgICAgICBwYWdlcy5wdXNoKGkpO1xuICAgICAgfVxuICAgIH1cbiAgICB0aGlzLnBhZ2VzID0gcGFnZXM7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICBnZXQgbGFzdEluZGV4KCk6IG51bWJlciB7XG4gICAgcmV0dXJuIE1hdGguY2VpbCh0aGlzLm56VG90YWwgLyB0aGlzLm56UGFnZVNpemUpO1xuICB9XG5cbiAgZ2V0IGlzTGFzdEluZGV4KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm56UGFnZUluZGV4ID09PSB0aGlzLmxhc3RJbmRleDtcbiAgfVxuXG4gIGdldCBpc0ZpcnN0SW5kZXgoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubnpQYWdlSW5kZXggPT09IHRoaXMuZmlyc3RJbmRleDtcbiAgfVxuXG4gIGdldCByYW5nZXMoKTogbnVtYmVyW10ge1xuICAgIHJldHVybiBbKHRoaXMubnpQYWdlSW5kZXggLSAxKSAqIHRoaXMubnpQYWdlU2l6ZSArIDEsIE1hdGgubWluKHRoaXMubnpQYWdlSW5kZXggKiB0aGlzLm56UGFnZVNpemUsIHRoaXMubnpUb3RhbCldO1xuICB9XG5cbiAgZ2V0IHNob3dBZGRPcHRpb24oKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubnpQYWdlU2l6ZU9wdGlvbnMuaW5kZXhPZih0aGlzLm56UGFnZVNpemUpID09PSAtMTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgaTE4bjogTnpJMThuU2VydmljZSwgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuaTE4bi5sb2NhbGVDaGFuZ2UucGlwZSh0YWtlVW50aWwodGhpcy4kZGVzdHJveSkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLmxvY2FsZSA9IHRoaXMuaTE4bi5nZXRMb2NhbGVEYXRhKCdQYWdpbmF0aW9uJyk7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9KTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuJGRlc3Ryb3kubmV4dCgpO1xuICAgIHRoaXMuJGRlc3Ryb3kuY29tcGxldGUoKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uelRvdGFsIHx8IGNoYW5nZXMubnpQYWdlU2l6ZSB8fCBjaGFuZ2VzLm56UGFnZUluZGV4KSB7XG4gICAgICB0aGlzLmJ1aWxkSW5kZXhlcygpO1xuICAgIH1cbiAgfVxufVxuIl19