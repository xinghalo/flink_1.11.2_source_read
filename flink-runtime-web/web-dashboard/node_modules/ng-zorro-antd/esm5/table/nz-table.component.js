/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { CdkVirtualScrollViewport } from '@angular/cdk/scrolling';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, ContentChildren, ElementRef, EventEmitter, Input, NgZone, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { fromEvent, merge, EMPTY, Subject } from 'rxjs';
import { flatMap, startWith, takeUntil } from 'rxjs/operators';
import { NzMeasureScrollbarService } from '../core/services/nz-measure-scrollbar.service';
import { InputBoolean, InputNumber } from '../core/util/convert';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { NzThComponent } from './nz-th.component';
import { NzVirtualScrollDirective } from './nz-virtual-scroll.directive';
/**
 * @template T
 */
var NzTableComponent = /** @class */ (function () {
    function NzTableComponent(renderer, ngZone, cdr, nzMeasureScrollbarService, i18n, elementRef) {
        this.renderer = renderer;
        this.ngZone = ngZone;
        this.cdr = cdr;
        this.nzMeasureScrollbarService = nzMeasureScrollbarService;
        this.i18n = i18n;
        /**
         * public data for ngFor tr
         */
        this.data = [];
        this.locale = {}; // tslint:disable-line:no-any
        this.lastScrollLeft = 0;
        this.headerBottomStyle = {};
        this.destroy$ = new Subject();
        this.nzSize = 'default';
        this.nzPageSizeOptions = [10, 20, 30, 40, 50];
        this.nzVirtualScroll = false;
        this.nzVirtualItemSize = 0;
        this.nzVirtualMaxBufferPx = 200;
        this.nzVirtualMinBufferPx = 100;
        this.nzLoadingDelay = 0;
        this.nzTotal = 0;
        this.nzWidthConfig = [];
        this.nzPageIndex = 1;
        this.nzPageSize = 10;
        this.nzData = [];
        this.nzPaginationPosition = 'bottom';
        this.nzScroll = { x: null, y: null };
        this.nzFrontPagination = true;
        this.nzTemplateMode = false;
        this.nzBordered = false;
        this.nzShowPagination = true;
        this.nzLoading = false;
        this.nzShowSizeChanger = false;
        this.nzHideOnSinglePage = false;
        this.nzShowQuickJumper = false;
        this.nzSimple = false;
        this.nzPageSizeChange = new EventEmitter();
        this.nzPageIndexChange = new EventEmitter();
        /* tslint:disable-next-line:no-any */
        this.nzCurrentPageDataChange = new EventEmitter();
        renderer.addClass(elementRef.nativeElement, 'ant-table-wrapper');
    }
    Object.defineProperty(NzTableComponent.prototype, "tableBodyNativeElement", {
        get: /**
         * @return {?}
         */
        function () {
            return this.tableBodyElement && this.tableBodyElement.nativeElement;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTableComponent.prototype, "tableHeaderNativeElement", {
        get: /**
         * @return {?}
         */
        function () {
            return this.tableHeaderElement && this.tableHeaderElement.nativeElement;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTableComponent.prototype, "cdkVirtualScrollNativeElement", {
        get: /**
         * @return {?}
         */
        function () {
            return this.cdkVirtualScrollElement && this.cdkVirtualScrollElement.nativeElement;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTableComponent.prototype, "mixTableBodyNativeElement", {
        get: /**
         * @return {?}
         */
        function () {
            return this.tableBodyNativeElement || this.cdkVirtualScrollNativeElement;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} size
     * @param {?} index
     * @return {?}
     */
    NzTableComponent.prototype.emitPageSizeOrIndex = /**
     * @param {?} size
     * @param {?} index
     * @return {?}
     */
    function (size, index) {
        if (this.nzPageSize !== size || this.nzPageIndex !== index) {
            if (this.nzPageSize !== size) {
                this.nzPageSize = size;
                this.nzPageSizeChange.emit(this.nzPageSize);
            }
            if (this.nzPageIndex !== index) {
                this.nzPageIndex = index;
                this.nzPageIndexChange.emit(this.nzPageIndex);
            }
            this.updateFrontPaginationDataIfNeeded(this.nzPageSize !== size);
        }
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzTableComponent.prototype.syncScrollTable = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (e.currentTarget === e.target) {
            /** @type {?} */
            var target = (/** @type {?} */ (e.target));
            if (target.scrollLeft !== this.lastScrollLeft && this.nzScroll && this.nzScroll.x) {
                if (target === this.mixTableBodyNativeElement && this.tableHeaderNativeElement) {
                    this.tableHeaderNativeElement.scrollLeft = target.scrollLeft;
                }
                else if (target === this.tableHeaderNativeElement && this.mixTableBodyNativeElement) {
                    this.mixTableBodyNativeElement.scrollLeft = target.scrollLeft;
                }
                this.setScrollPositionClassName();
            }
            this.lastScrollLeft = target.scrollLeft;
        }
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.setScrollPositionClassName = /**
     * @return {?}
     */
    function () {
        if (this.mixTableBodyNativeElement && this.nzScroll && this.nzScroll.x) {
            if (this.mixTableBodyNativeElement.scrollWidth === this.mixTableBodyNativeElement.clientWidth &&
                this.mixTableBodyNativeElement.scrollWidth !== 0) {
                this.setScrollName();
            }
            else if (this.mixTableBodyNativeElement.scrollLeft === 0) {
                this.setScrollName('left');
            }
            else if (this.mixTableBodyNativeElement.scrollWidth ===
                this.mixTableBodyNativeElement.scrollLeft + this.mixTableBodyNativeElement.clientWidth) {
                this.setScrollName('right');
            }
            else {
                this.setScrollName('middle');
            }
        }
    };
    /**
     * @param {?=} position
     * @return {?}
     */
    NzTableComponent.prototype.setScrollName = /**
     * @param {?=} position
     * @return {?}
     */
    function (position) {
        var _this = this;
        /** @type {?} */
        var prefix = 'ant-table-scroll-position';
        /** @type {?} */
        var classList = ['left', 'right', 'middle'];
        classList.forEach((/**
         * @param {?} name
         * @return {?}
         */
        function (name) {
            _this.renderer.removeClass(_this.tableMainElement.nativeElement, prefix + "-" + name);
        }));
        if (position) {
            this.renderer.addClass(this.tableMainElement.nativeElement, prefix + "-" + position);
        }
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.fitScrollBar = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var scrollbarWidth = this.nzMeasureScrollbarService.scrollBarWidth;
        if (scrollbarWidth) {
            this.headerBottomStyle = {
                marginBottom: "-" + scrollbarWidth + "px",
                paddingBottom: "0px"
            };
            this.cdr.markForCheck();
        }
    };
    /**
     * @param {?=} isPageSizeOrDataChange
     * @return {?}
     */
    NzTableComponent.prototype.updateFrontPaginationDataIfNeeded = /**
     * @param {?=} isPageSizeOrDataChange
     * @return {?}
     */
    function (isPageSizeOrDataChange) {
        var _this = this;
        if (isPageSizeOrDataChange === void 0) { isPageSizeOrDataChange = false; }
        /** @type {?} */
        var data = [];
        if (this.nzFrontPagination) {
            this.nzTotal = this.nzData.length;
            if (isPageSizeOrDataChange) {
                /** @type {?} */
                var maxPageIndex = Math.ceil(this.nzData.length / this.nzPageSize) || 1;
                /** @type {?} */
                var pageIndex_1 = this.nzPageIndex > maxPageIndex ? maxPageIndex : this.nzPageIndex;
                if (pageIndex_1 !== this.nzPageIndex) {
                    this.nzPageIndex = pageIndex_1;
                    Promise.resolve().then((/**
                     * @return {?}
                     */
                    function () { return _this.nzPageIndexChange.emit(pageIndex_1); }));
                }
            }
            data = this.nzData.slice((this.nzPageIndex - 1) * this.nzPageSize, this.nzPageIndex * this.nzPageSize);
        }
        else {
            data = this.nzData;
        }
        this.data = tslib_1.__spread(data);
        this.nzCurrentPageDataChange.next(this.data);
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.ngOnInit = /**
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
    NzTableComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzScroll) {
            if (changes.nzScroll.currentValue) {
                this.nzScroll = changes.nzScroll.currentValue;
            }
            else {
                this.nzScroll = { x: null, y: null };
            }
            this.setScrollPositionClassName();
        }
        if (changes.nzPageIndex || changes.nzPageSize || changes.nzFrontPagination || changes.nzData) {
            this.updateFrontPaginationDataIfNeeded(!!(changes.nzPageSize || changes.nzData));
        }
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        setTimeout((/**
         * @return {?}
         */
        function () { return _this.setScrollPositionClassName(); }));
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        function () {
            merge(_this.tableHeaderNativeElement ? fromEvent(_this.tableHeaderNativeElement, 'scroll') : EMPTY, _this.mixTableBodyNativeElement ? fromEvent(_this.mixTableBodyNativeElement, 'scroll') : EMPTY)
                .pipe(takeUntil(_this.destroy$))
                .subscribe((/**
             * @param {?} data
             * @return {?}
             */
            function (data) {
                _this.syncScrollTable(data);
            }));
            fromEvent(window, 'resize')
                .pipe(startWith(true), takeUntil(_this.destroy$))
                .subscribe((/**
             * @return {?}
             */
            function () {
                _this.fitScrollBar();
                _this.setScrollPositionClassName();
            }));
        }));
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.listOfNzThComponent.changes
            .pipe(startWith(true), flatMap((/**
         * @return {?}
         */
        function () {
            return merge.apply(void 0, tslib_1.__spread([_this.listOfNzThComponent.changes], _this.listOfNzThComponent.map((/**
             * @param {?} th
             * @return {?}
             */
            function (th) { return th.nzWidthChange$; }))));
        })), takeUntil(this.destroy$))
            .subscribe((/**
         * @return {?}
         */
        function () {
            _this.cdr.markForCheck();
        }));
    };
    /**
     * @return {?}
     */
    NzTableComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzTableComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-table',
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<ng-template #renderItemTemplate let-type let-page=\"page\">\n  <a class=\"ant-pagination-item-link\" *ngIf=\"type==='pre'\"><i nz-icon type=\"left\"></i></a>\n  <a class=\"ant-pagination-item-link\" *ngIf=\"type==='next'\"><i nz-icon type=\"right\"></i></a>\n  <a *ngIf=\"type=='page'\">{{ page }}</a>\n</ng-template>\n<ng-template #colGroupTemplate>\n  <colgroup>\n    <col [style.width]=\"width\" [style.minWidth]=\"width\" *ngFor=\"let width of nzWidthConfig\">\n    <col [style.width]=\"th.nzWidth\" [style.minWidth]=\"th.nzWidth\" *ngFor=\"let th of listOfNzThComponent\">\n  </colgroup>\n</ng-template>\n<ng-template #headerTemplate>\n  <ng-template [ngTemplateOutlet]=\"colGroupTemplate\"></ng-template>\n  <thead class=\"ant-table-thead\" *ngIf=\"!nzScroll.y\">\n    <ng-template [ngTemplateOutlet]=\"nzTheadComponent?.templateRef\"></ng-template>\n  </thead>\n</ng-template>\n<ng-template #tableInnerTemplate>\n  <div #tableHeaderElement\n    *ngIf=\"nzScroll.x || nzScroll.y\"\n    [ngStyle]=\"headerBottomStyle\"\n    class=\"ant-table-header\">\n    <table [class.ant-table-fixed]=\"nzScroll.x\" [style.width]=\"nzScroll.x\">\n      <ng-template [ngTemplateOutlet]=\"colGroupTemplate\"></ng-template>\n      <thead class=\"ant-table-thead\" *ngIf=\"nzScroll.y\">\n        <ng-template [ngTemplateOutlet]=\"nzTheadComponent?.templateRef\"></ng-template>\n      </thead>\n    </table>\n  </div>\n  <div #tableBodyElement *ngIf=\"!nzVirtualScroll;else scrollViewTpl\"\n    class=\"ant-table-body\"\n    [style.maxHeight]=\"nzScroll.y\"\n    [style.overflow-y]=\"nzScroll.y ? 'scroll' : ''\"\n    [style.overflow-x]=\"nzScroll.x ? 'auto' : ''\">\n    <table [class.ant-table-fixed]=\"nzScroll.x\" [style.width]=\"nzScroll.x\">\n      <ng-template [ngIf]=\"!nzVirtualScroll\" [ngTemplateOutlet]=\"headerTemplate\"></ng-template>\n      <ng-content></ng-content>\n    </table>\n  </div>\n  <ng-template #scrollViewTpl>\n    <cdk-virtual-scroll-viewport\n      class=\"ant-table-body\"\n      [itemSize]=\"nzVirtualItemSize\"\n      [maxBufferPx]=\"nzVirtualMaxBufferPx\"\n      [minBufferPx]=\"nzVirtualMinBufferPx\"\n      [style.height]=\"nzScroll.y\">\n      <table [class.ant-table-fixed]=\"nzScroll.x\" [style.width]=\"nzScroll.x\">\n        <ng-template [ngIf]=\"nzVirtualScroll\" [ngTemplateOutlet]=\"headerTemplate\"></ng-template>\n        <tbody>\n          <ng-container *cdkVirtualFor=\"let item of data; let i = index\">\n            <ng-template [ngTemplateOutlet]=\"nzVirtualScrollDirective?.templateRef\" [ngTemplateOutletContext]=\"{$implicit:item, index:i}\"></ng-template>\n          </ng-container>\n        </tbody>\n      </table>\n    </cdk-virtual-scroll-viewport>\n  </ng-template>\n  <div class=\"ant-table-placeholder\" *ngIf=\"data.length === 0 && !nzLoading && !nzTemplateMode\">\n    <nz-embed-empty [nzComponentName]=\"'table'\" [specificContent]=\"nzNoResult\"></nz-embed-empty>\n  </div>\n  <div class=\"ant-table-footer\" *ngIf=\"nzFooter\">\n    <ng-container *nzStringTemplateOutlet=\"nzFooter\">{{ nzFooter }}</ng-container>\n  </div>\n</ng-template>\n<ng-template #paginationTemplate>\n  <nz-pagination *ngIf=\"nzShowPagination && data.length\"\n    [nzInTable]=\"true\"\n    [nzShowSizeChanger]=\"nzShowSizeChanger\"\n    [nzPageSizeOptions]=\"nzPageSizeOptions\"\n    [nzItemRender]=\"nzItemRender\"\n    [nzShowQuickJumper]=\"nzShowQuickJumper\"\n    [nzHideOnSinglePage]=\"nzHideOnSinglePage\"\n    [nzShowTotal]=\"nzShowTotal\"\n    [nzSize]=\"(nzSize === 'middle' || nzSize=='small') ? 'small' : ''\"\n    [nzPageSize]=\"nzPageSize\"\n    [nzTotal]=\"nzTotal\"\n    [nzSimple]=\"nzSimple\"\n    [nzPageIndex]=\"nzPageIndex\"\n    (nzPageSizeChange)=\"emitPageSizeOrIndex($event,nzPageIndex)\"\n    (nzPageIndexChange)=\"emitPageSizeOrIndex(nzPageSize,$event)\">\n  </nz-pagination>\n</ng-template>\n<nz-spin [nzDelay]=\"nzLoadingDelay\" [nzSpinning]=\"nzLoading\">\n  <ng-container *ngIf=\"nzPaginationPosition === 'both' || nzPaginationPosition === 'top'\">\n    <ng-template [ngTemplateOutlet]=\"paginationTemplate\"></ng-template>\n  </ng-container>\n  <div #tableMainElement\n    class=\"ant-table\"\n    [class.ant-table-fixed-header]=\"nzScroll.x || nzScroll.y\"\n    [class.ant-table-bordered]=\"nzBordered\"\n    [class.ant-table-default]=\"nzSize === 'default'\"\n    [class.ant-table-middle]=\"nzSize === 'middle'\"\n    [class.ant-table-small]=\"nzSize === 'small'\">\n    <div class=\"ant-table-title\" *ngIf=\"nzTitle\">\n      <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n    </div>\n    <div class=\"ant-table-content\">\n      <ng-container *ngIf=\"nzScroll.x || nzScroll.y; else tableInnerTemplate\">\n        <div class=\"ant-table-scroll\">\n          <ng-template [ngTemplateOutlet]=\"tableInnerTemplate\"></ng-template>\n        </div>\n      </ng-container>\n    </div>\n  </div>\n  <ng-container *ngIf=\"nzPaginationPosition === 'both' || nzPaginationPosition === 'bottom'\">\n    <ng-template [ngTemplateOutlet]=\"paginationTemplate\"></ng-template>\n  </ng-container>\n</nz-spin>\n",
                    host: {
                        '[class.ant-table-empty]': 'data.length === 0'
                    },
                    styles: ["\n      nz-table {\n        display: block;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzTableComponent.ctorParameters = function () { return [
        { type: Renderer2 },
        { type: NgZone },
        { type: ChangeDetectorRef },
        { type: NzMeasureScrollbarService },
        { type: NzI18nService },
        { type: ElementRef }
    ]; };
    NzTableComponent.propDecorators = {
        listOfNzThComponent: [{ type: ContentChildren, args: [NzThComponent, { descendants: true },] }],
        tableHeaderElement: [{ type: ViewChild, args: ['tableHeaderElement', { read: ElementRef },] }],
        tableBodyElement: [{ type: ViewChild, args: ['tableBodyElement', { read: ElementRef },] }],
        tableMainElement: [{ type: ViewChild, args: ['tableMainElement', { read: ElementRef },] }],
        cdkVirtualScrollElement: [{ type: ViewChild, args: [CdkVirtualScrollViewport, { read: ElementRef },] }],
        nzVirtualScrollDirective: [{ type: ContentChild, args: [NzVirtualScrollDirective,] }],
        nzSize: [{ type: Input }],
        nzShowTotal: [{ type: Input }],
        nzPageSizeOptions: [{ type: Input }],
        nzVirtualScroll: [{ type: Input }],
        nzVirtualItemSize: [{ type: Input }],
        nzVirtualMaxBufferPx: [{ type: Input }],
        nzVirtualMinBufferPx: [{ type: Input }],
        nzLoadingDelay: [{ type: Input }],
        nzTotal: [{ type: Input }],
        nzTitle: [{ type: Input }],
        nzFooter: [{ type: Input }],
        nzNoResult: [{ type: Input }],
        nzWidthConfig: [{ type: Input }],
        nzPageIndex: [{ type: Input }],
        nzPageSize: [{ type: Input }],
        nzData: [{ type: Input }],
        nzPaginationPosition: [{ type: Input }],
        nzScroll: [{ type: Input }],
        nzItemRender: [{ type: Input }, { type: ViewChild, args: ['renderItemTemplate',] }],
        nzFrontPagination: [{ type: Input }],
        nzTemplateMode: [{ type: Input }],
        nzBordered: [{ type: Input }],
        nzShowPagination: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzShowSizeChanger: [{ type: Input }],
        nzHideOnSinglePage: [{ type: Input }],
        nzShowQuickJumper: [{ type: Input }],
        nzSimple: [{ type: Input }],
        nzPageSizeChange: [{ type: Output }],
        nzPageIndexChange: [{ type: Output }],
        nzCurrentPageDataChange: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzVirtualScroll", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzVirtualItemSize", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzVirtualMaxBufferPx", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzVirtualMinBufferPx", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzFrontPagination", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzTemplateMode", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzBordered", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzShowPagination", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzLoading", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzShowSizeChanger", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzHideOnSinglePage", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzShowQuickJumper", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTableComponent.prototype, "nzSimple", void 0);
    return NzTableComponent;
}());
export { NzTableComponent };
if (false) {
    /**
     * public data for ngFor tr
     * @type {?}
     */
    NzTableComponent.prototype.data;
    /** @type {?} */
    NzTableComponent.prototype.locale;
    /** @type {?} */
    NzTableComponent.prototype.nzTheadComponent;
    /** @type {?} */
    NzTableComponent.prototype.lastScrollLeft;
    /** @type {?} */
    NzTableComponent.prototype.headerBottomStyle;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.destroy$;
    /** @type {?} */
    NzTableComponent.prototype.listOfNzThComponent;
    /** @type {?} */
    NzTableComponent.prototype.tableHeaderElement;
    /** @type {?} */
    NzTableComponent.prototype.tableBodyElement;
    /** @type {?} */
    NzTableComponent.prototype.tableMainElement;
    /** @type {?} */
    NzTableComponent.prototype.cdkVirtualScrollElement;
    /** @type {?} */
    NzTableComponent.prototype.nzVirtualScrollDirective;
    /** @type {?} */
    NzTableComponent.prototype.nzSize;
    /** @type {?} */
    NzTableComponent.prototype.nzShowTotal;
    /** @type {?} */
    NzTableComponent.prototype.nzPageSizeOptions;
    /** @type {?} */
    NzTableComponent.prototype.nzVirtualScroll;
    /** @type {?} */
    NzTableComponent.prototype.nzVirtualItemSize;
    /** @type {?} */
    NzTableComponent.prototype.nzVirtualMaxBufferPx;
    /** @type {?} */
    NzTableComponent.prototype.nzVirtualMinBufferPx;
    /** @type {?} */
    NzTableComponent.prototype.nzLoadingDelay;
    /** @type {?} */
    NzTableComponent.prototype.nzTotal;
    /** @type {?} */
    NzTableComponent.prototype.nzTitle;
    /** @type {?} */
    NzTableComponent.prototype.nzFooter;
    /** @type {?} */
    NzTableComponent.prototype.nzNoResult;
    /** @type {?} */
    NzTableComponent.prototype.nzWidthConfig;
    /** @type {?} */
    NzTableComponent.prototype.nzPageIndex;
    /** @type {?} */
    NzTableComponent.prototype.nzPageSize;
    /** @type {?} */
    NzTableComponent.prototype.nzData;
    /** @type {?} */
    NzTableComponent.prototype.nzPaginationPosition;
    /** @type {?} */
    NzTableComponent.prototype.nzScroll;
    /** @type {?} */
    NzTableComponent.prototype.nzItemRender;
    /** @type {?} */
    NzTableComponent.prototype.nzFrontPagination;
    /** @type {?} */
    NzTableComponent.prototype.nzTemplateMode;
    /** @type {?} */
    NzTableComponent.prototype.nzBordered;
    /** @type {?} */
    NzTableComponent.prototype.nzShowPagination;
    /** @type {?} */
    NzTableComponent.prototype.nzLoading;
    /** @type {?} */
    NzTableComponent.prototype.nzShowSizeChanger;
    /** @type {?} */
    NzTableComponent.prototype.nzHideOnSinglePage;
    /** @type {?} */
    NzTableComponent.prototype.nzShowQuickJumper;
    /** @type {?} */
    NzTableComponent.prototype.nzSimple;
    /** @type {?} */
    NzTableComponent.prototype.nzPageSizeChange;
    /** @type {?} */
    NzTableComponent.prototype.nzPageIndexChange;
    /** @type {?} */
    NzTableComponent.prototype.nzCurrentPageDataChange;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.nzMeasureScrollbarService;
    /**
     * @type {?}
     * @private
     */
    NzTableComponent.prototype.i18n;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFibGUuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRhYmxlLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ2xFLE9BQU8sRUFHTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osZUFBZSxFQUNmLFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUNMLE1BQU0sRUFJTixNQUFNLEVBQ04sU0FBUyxFQUNULFNBQVMsRUFFVCxXQUFXLEVBQ1gsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsU0FBUyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3hELE9BQU8sRUFBRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQy9ELE9BQU8sRUFBRSx5QkFBeUIsRUFBRSxNQUFNLCtDQUErQyxDQUFDO0FBRTFGLE9BQU8sRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDakUsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBQ3hELE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUVsRCxPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSwrQkFBK0IsQ0FBQzs7OztBQUV6RTtJQStLRSwwQkFDVSxRQUFtQixFQUNuQixNQUFjLEVBQ2QsR0FBc0IsRUFDdEIseUJBQW9ELEVBQ3BELElBQW1CLEVBQzNCLFVBQXNCO1FBTGQsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQUNuQixXQUFNLEdBQU4sTUFBTSxDQUFRO1FBQ2QsUUFBRyxHQUFILEdB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O2dFQUE0QjtJQUMzQjtRQUFmLFlBQVksRUFBRTs7K0RBQTJCO0lBQzFCO1FBQWYsWUFBWSxFQUFFOztzREFBa0I7SUEwTDVDLHVCQUFDO0NBQUEsQUF4UEQsSUF3UEM7U0F0T1ksZ0JBQWdCOzs7Ozs7SUFFM0IsZ0NBQWU7O0lBQ2Ysa0NBQWlCOztJQUNqQiw0Q0FBbUM7O0lBQ25DLDBDQUFtQjs7SUFDbkIsNkNBQXVCOzs7OztJQUN2QixvQ0FBdUM7O0lBQ3ZDLCtDQUFxRzs7SUFDckcsOENBQXNGOztJQUN0Riw0Q0FBa0Y7O0lBQ2xGLDRDQUFrRjs7SUFDbEYsbURBQStGOztJQUMvRixvREFBMkY7O0lBQzNGLGtDQUEyQzs7SUFDM0MsdUNBQWtGOztJQUNsRiw2Q0FBa0Q7O0lBQ2xELDJDQUFpRDs7SUFDakQsNkNBQThDOztJQUM5QyxnREFBbUQ7O0lBQ25ELGdEQUFtRDs7SUFDbkQsMENBQTRCOztJQUM1QixtQ0FBcUI7O0lBQ3JCLG1DQUE2Qzs7SUFDN0Msb0NBQThDOztJQUM5QyxzQ0FBZ0Q7O0lBQ2hELHlDQUFzQzs7SUFDdEMsdUNBQXlCOztJQUN6QixzQ0FBeUI7O0lBQ3pCLGtDQUEwQjs7SUFDMUIsZ0RBQW9FOztJQUNwRSxvQ0FBbUY7O0lBQ25GLHdDQUdHOztJQUNILDZDQUFrRDs7SUFDbEQsMENBQWdEOztJQUNoRCxzQ0FBNEM7O0lBQzVDLDRDQUFpRDs7SUFDakQscUNBQTJDOztJQUMzQyw2Q0FBbUQ7O0lBQ25ELDhDQUFvRDs7SUFDcEQsNkNBQW1EOztJQUNuRCxvQ0FBMEM7O0lBQzFDLDRDQUErRTs7SUFDL0UsNkNBQWdGOztJQUVoRixtREFBcUY7Ozs7O0lBOEduRixvQ0FBMkI7Ozs7O0lBQzNCLGtDQUFzQjs7Ozs7SUFDdEIsK0JBQThCOzs7OztJQUM5QixxREFBNEQ7Ozs7O0lBQzVELGdDQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENka1ZpcnR1YWxTY3JvbGxWaWV3cG9ydCB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9zY3JvbGxpbmcnO1xuaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZCxcbiAgQ29udGVudENoaWxkcmVuLFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBOZ1pvbmUsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgUXVlcnlMaXN0LFxuICBSZW5kZXJlcjIsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgZnJvbUV2ZW50LCBtZXJnZSwgRU1QVFksIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGZsYXRNYXAsIHN0YXJ0V2l0aCwgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpNZWFzdXJlU2Nyb2xsYmFyU2VydmljZSB9IGZyb20gJy4uL2NvcmUvc2VydmljZXMvbnotbWVhc3VyZS1zY3JvbGxiYXIuc2VydmljZSc7XG5pbXBvcnQgeyBOelNpemVNRFNUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9zaXplJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiwgSW5wdXROdW1iZXIgfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBOekkxOG5TZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpUaENvbXBvbmVudCB9IGZyb20gJy4vbnotdGguY29tcG9uZW50JztcbmltcG9ydCB7IE56VGhlYWRDb21wb25lbnQgfSBmcm9tICcuL256LXRoZWFkLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelZpcnR1YWxTY3JvbGxEaXJlY3RpdmUgfSBmcm9tICcuL256LXZpcnR1YWwtc2Nyb2xsLmRpcmVjdGl2ZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LXRhYmxlJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdGFibGUuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1tjbGFzcy5hbnQtdGFibGUtZW1wdHldJzogJ2RhdGEubGVuZ3RoID09PSAwJ1xuICB9LFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei10YWJsZSB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbi8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZSBuby1hbnlcbmV4cG9ydCBjbGFzcyBOelRhYmxlQ29tcG9uZW50PFQgPSBhbnk+IGltcGxlbWVudHMgT25Jbml0LCBBZnRlclZpZXdJbml0LCBPbkRlc3Ryb3ksIE9uQ2hhbmdlcywgQWZ0ZXJDb250ZW50SW5pdCB7XG4gIC8qKiBwdWJsaWMgZGF0YSBmb3IgbmdGb3IgdHIgKi9cbiAgZGF0YTogVFtdID0gW107XG4gIGxvY2FsZTogYW55ID0ge307IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gIG56VGhlYWRDb21wb25lbnQ6IE56VGhlYWRDb21wb25lbnQ7XG4gIGxhc3RTY3JvbGxMZWZ0ID0gMDtcbiAgaGVhZGVyQm90dG9tU3R5bGUgPSB7fTtcbiAgcHJpdmF0ZSBkZXN0cm95JCA9IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpUaENvbXBvbmVudCwgeyBkZXNjZW5kYW50czogdHJ1ZSB9KSBsaXN0T2ZOelRoQ29tcG9uZW50OiBRdWVyeUxpc3Q8TnpUaENvbXBvbmVudD47XG4gIEBWaWV3Q2hpbGQoJ3RhYmxlSGVhZGVyRWxlbWVudCcsIHsgcmVhZDogRWxlbWVudFJlZiB9KSB0YWJsZUhlYWRlckVsZW1lbnQ6IEVsZW1lbnRSZWY7XG4gIEBWaWV3Q2hpbGQoJ3RhYmxlQm9keUVsZW1lbnQnLCB7IHJlYWQ6IEVsZW1lbnRSZWYgfSkgdGFibGVCb2R5RWxlbWVudDogRWxlbWVudFJlZjtcbiAgQFZpZXdDaGlsZCgndGFibGVNYWluRWxlbWVudCcsIHsgcmVhZDogRWxlbWVudFJlZiB9KSB0YWJsZU1haW5FbGVtZW50OiBFbGVtZW50UmVmO1xuICBAVmlld0NoaWxkKENka1ZpcnR1YWxTY3JvbGxWaWV3cG9ydCwgeyByZWFkOiBFbGVtZW50UmVmIH0pIGNka1ZpcnR1YWxTY3JvbGxFbGVtZW50OiBFbGVtZW50UmVmO1xuICBAQ29udGVudENoaWxkKE56VmlydHVhbFNjcm9sbERpcmVjdGl2ZSkgbnpWaXJ0dWFsU2Nyb2xsRGlyZWN0aXZlOiBOelZpcnR1YWxTY3JvbGxEaXJlY3RpdmU7XG4gIEBJbnB1dCgpIG56U2l6ZTogTnpTaXplTURTVHlwZSA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpTaG93VG90YWw6IFRlbXBsYXRlUmVmPHsgJGltcGxpY2l0OiBudW1iZXI7IHJhbmdlOiBbbnVtYmVyLCBudW1iZXJdIH0+O1xuICBASW5wdXQoKSBuelBhZ2VTaXplT3B0aW9ucyA9IFsxMCwgMjAsIDMwLCA0MCwgNTBdO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpWaXJ0dWFsU2Nyb2xsID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56VmlydHVhbEl0ZW1TaXplID0gMDtcbiAgQElucHV0KCkgQElucHV0TnVtYmVyKCkgbnpWaXJ0dWFsTWF4QnVmZmVyUHggPSAyMDA7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56VmlydHVhbE1pbkJ1ZmZlclB4ID0gMTAwO1xuICBASW5wdXQoKSBuekxvYWRpbmdEZWxheSA9IDA7XG4gIEBJbnB1dCgpIG56VG90YWwgPSAwO1xuICBASW5wdXQoKSBuelRpdGxlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpGb290ZXI6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuek5vUmVzdWx0OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpXaWR0aENvbmZpZzogc3RyaW5nW10gPSBbXTtcbiAgQElucHV0KCkgbnpQYWdlSW5kZXggPSAxO1xuICBASW5wdXQoKSBuelBhZ2VTaXplID0gMTA7XG4gIEBJbnB1dCgpIG56RGF0YTogVFtdID0gW107XG4gIEBJbnB1dCgpIG56UGFnaW5hdGlvblBvc2l0aW9uOiAndG9wJyB8ICdib3R0b20nIHwgJ2JvdGgnID0gJ2JvdHRvbSc7XG4gIEBJbnB1dCgpIG56U2Nyb2xsOiB7IHg/OiBzdHJpbmcgfCBudWxsOyB5Pzogc3RyaW5nIHwgbnVsbCB9ID0geyB4OiBudWxsLCB5OiBudWxsIH07XG4gIEBJbnB1dCgpIEBWaWV3Q2hpbGQoJ3JlbmRlckl0ZW1UZW1wbGF0ZScpIG56SXRlbVJlbmRlcjogVGVtcGxhdGVSZWY8e1xuICAgICRpbXBsaWNpdDogJ3BhZ2UnIHwgJ3ByZXYnIHwgJ25leHQnO1xuICAgIHBhZ2U6IG51bWJlcjtcbiAgfT47XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekZyb250UGFnaW5hdGlvbiA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelRlbXBsYXRlTW9kZSA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpCb3JkZXJlZCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTaG93UGFnaW5hdGlvbiA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekxvYWRpbmcgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1NpemVDaGFuZ2VyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekhpZGVPblNpbmdsZVBhZ2UgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1F1aWNrSnVtcGVyID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNpbXBsZSA9IGZhbHNlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpQYWdlU2l6ZUNoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuelBhZ2VJbmRleENoYW5nZTogRXZlbnRFbWl0dGVyPG51bWJlcj4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG4gIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Q3VycmVudFBhZ2VEYXRhQ2hhbmdlOiBFdmVudEVtaXR0ZXI8YW55W10+ID0gbmV3IEV2ZW50RW1pdHRlcigpO1xuXG4gIGdldCB0YWJsZUJvZHlOYXRpdmVFbGVtZW50KCk6IEhUTUxFbGVtZW50IHtcbiAgICByZXR1cm4gdGhpcy50YWJsZUJvZHlFbGVtZW50ICYmIHRoaXMudGFibGVCb2R5RWxlbWVudC5uYXRpdmVFbGVtZW50O1xuICB9XG5cbiAgZ2V0IHRhYmxlSGVhZGVyTmF0aXZlRWxlbWVudCgpOiBIVE1MRWxlbWVudCB7XG4gICAgcmV0dXJuIHRoaXMudGFibGVIZWFkZXJFbGVtZW50ICYmIHRoaXMudGFibGVIZWFkZXJFbGVtZW50Lm5hdGl2ZUVsZW1lbnQ7XG4gIH1cblxuICBnZXQgY2RrVmlydHVhbFNjcm9sbE5hdGl2ZUVsZW1lbnQoKTogSFRNTEVsZW1lbnQge1xuICAgIHJldHVybiB0aGlzLmNka1ZpcnR1YWxTY3JvbGxFbGVtZW50ICYmIHRoaXMuY2RrVmlydHVhbFNjcm9sbEVsZW1lbnQubmF0aXZlRWxlbWVudDtcbiAgfVxuXG4gIGdldCBtaXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50KCk6IEhUTUxFbGVtZW50IHtcbiAgICByZXR1cm4gdGhpcy50YWJsZUJvZHlOYXRpdmVFbGVtZW50IHx8IHRoaXMuY2RrVmlydHVhbFNjcm9sbE5hdGl2ZUVsZW1lbnQ7XG4gIH1cblxuICBlbWl0UGFnZVNpemVPckluZGV4KHNpemU6IG51bWJlciwgaW5kZXg6IG51bWJlcik6IHZvaWQge1xuICAgIGlmICh0aGlzLm56UGFnZVNpemUgIT09IHNpemUgfHwgdGhpcy5uelBhZ2VJbmRleCAhPT0gaW5kZXgpIHtcbiAgICAgIGlmICh0aGlzLm56UGFnZVNpemUgIT09IHNpemUpIHtcbiAgICAgICAgdGhpcy5uelBhZ2VTaXplID0gc2l6ZTtcbiAgICAgICAgdGhpcy5uelBhZ2VTaXplQ2hhbmdlLmVtaXQodGhpcy5uelBhZ2VTaXplKTtcbiAgICAgIH1cbiAgICAgIGlmICh0aGlzLm56UGFnZUluZGV4ICE9PSBpbmRleCkge1xuICAgICAgICB0aGlzLm56UGFnZUluZGV4ID0gaW5kZXg7XG4gICAgICAgIHRoaXMubnpQYWdlSW5kZXhDaGFuZ2UuZW1pdCh0aGlzLm56UGFnZUluZGV4KTtcbiAgICAgIH1cbiAgICAgIHRoaXMudXBkYXRlRnJvbnRQYWdpbmF0aW9uRGF0YUlmTmVlZGVkKHRoaXMubnpQYWdlU2l6ZSAhPT0gc2l6ZSk7XG4gICAgfVxuICB9XG5cbiAgc3luY1Njcm9sbFRhYmxlKGU6IE1vdXNlRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAoZS5jdXJyZW50VGFyZ2V0ID09PSBlLnRhcmdldCkge1xuICAgICAgY29uc3QgdGFyZ2V0ID0gZS50YXJnZXQgYXMgSFRNTEVsZW1lbnQ7XG4gICAgICBpZiAodGFyZ2V0LnNjcm9sbExlZnQgIT09IHRoaXMubGFzdFNjcm9sbExlZnQgJiYgdGhpcy5uelNjcm9sbCAmJiB0aGlzLm56U2Nyb2xsLngpIHtcbiAgICAgICAgaWYgKHRhcmdldCA9PT0gdGhpcy5taXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50ICYmIHRoaXMudGFibGVIZWFkZXJOYXRpdmVFbGVtZW50KSB7XG4gICAgICAgICAgdGhpcy50YWJsZUhlYWRlck5hdGl2ZUVsZW1lbnQuc2Nyb2xsTGVmdCA9IHRhcmdldC5zY3JvbGxMZWZ0O1xuICAgICAgICB9IGVsc2UgaWYgKHRhcmdldCA9PT0gdGhpcy50YWJsZUhlYWRlck5hdGl2ZUVsZW1lbnQgJiYgdGhpcy5taXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50KSB7XG4gICAgICAgICAgdGhpcy5taXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50LnNjcm9sbExlZnQgPSB0YXJnZXQuc2Nyb2xsTGVmdDtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLnNldFNjcm9sbFBvc2l0aW9uQ2xhc3NOYW1lKCk7XG4gICAgICB9XG4gICAgICB0aGlzLmxhc3RTY3JvbGxMZWZ0ID0gdGFyZ2V0LnNjcm9sbExlZnQ7XG4gICAgfVxuICB9XG5cbiAgc2V0U2Nyb2xsUG9zaXRpb25DbGFzc05hbWUoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubWl4VGFibGVCb2R5TmF0aXZlRWxlbWVudCAmJiB0aGlzLm56U2Nyb2xsICYmIHRoaXMubnpTY3JvbGwueCkge1xuICAgICAgaWYgKFxuICAgICAgICB0aGlzLm1peFRhYmxlQm9keU5hdGl2ZUVsZW1lbnQuc2Nyb2xsV2lkdGggPT09IHRoaXMubWl4VGFibGVCb2R5TmF0aXZlRWxlbWVudC5jbGllbnRXaWR0aCAmJlxuICAgICAgICB0aGlzLm1peFRhYmxlQm9keU5hdGl2ZUVsZW1lbnQuc2Nyb2xsV2lkdGggIT09IDBcbiAgICAgICkge1xuICAgICAgICB0aGlzLnNldFNjcm9sbE5hbWUoKTtcbiAgICAgIH0gZWxzZSBpZiAodGhpcy5taXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50LnNjcm9sbExlZnQgPT09IDApIHtcbiAgICAgICAgdGhpcy5zZXRTY3JvbGxOYW1lKCdsZWZ0Jyk7XG4gICAgICB9IGVsc2UgaWYgKFxuICAgICAgICB0aGlzLm1peFRhYmxlQm9keU5hdGl2ZUVsZW1lbnQuc2Nyb2xsV2lkdGggPT09XG4gICAgICAgIHRoaXMubWl4VGFibGVCb2R5TmF0aXZlRWxlbWVudC5zY3JvbGxMZWZ0ICsgdGhpcy5taXhUYWJsZUJvZHlOYXRpdmVFbGVtZW50LmNsaWVudFdpZHRoXG4gICAgICApIHtcbiAgICAgICAgdGhpcy5zZXRTY3JvbGxOYW1lKCdyaWdodCcpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5zZXRTY3JvbGxOYW1lKCdtaWRkbGUnKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBzZXRTY3JvbGxOYW1lKHBvc2l0aW9uPzogc3RyaW5nKTogdm9pZCB7XG4gICAgY29uc3QgcHJlZml4ID0gJ2FudC10YWJsZS1zY3JvbGwtcG9zaXRpb24nO1xuICAgIGNvbnN0IGNsYXNzTGlzdCA9IFsnbGVmdCcsICdyaWdodCcsICdtaWRkbGUnXTtcbiAgICBjbGFzc0xpc3QuZm9yRWFjaChuYW1lID0+IHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2xhc3ModGhpcy50YWJsZU1haW5FbGVtZW50Lm5hdGl2ZUVsZW1lbnQsIGAke3ByZWZpeH0tJHtuYW1lfWApO1xuICAgIH0pO1xuICAgIGlmIChwb3NpdGlvbikge1xuICAgICAgdGhpcy5yZW5kZXJlci5hZGRDbGFzcyh0aGlzLnRhYmxlTWFpbkVsZW1lbnQubmF0aXZlRWxlbWVudCwgYCR7cHJlZml4fS0ke3Bvc2l0aW9ufWApO1xuICAgIH1cbiAgfVxuXG4gIGZpdFNjcm9sbEJhcigpOiB2b2lkIHtcbiAgICBjb25zdCBzY3JvbGxiYXJXaWR0aCA9IHRoaXMubnpNZWFzdXJlU2Nyb2xsYmFyU2VydmljZS5zY3JvbGxCYXJXaWR0aDtcbiAgICBpZiAoc2Nyb2xsYmFyV2lkdGgpIHtcbiAgICAgIHRoaXMuaGVhZGVyQm90dG9tU3R5bGUgPSB7XG4gICAgICAgIG1hcmdpbkJvdHRvbTogYC0ke3Njcm9sbGJhcldpZHRofXB4YCxcbiAgICAgICAgcGFkZGluZ0JvdHRvbTogYDBweGBcbiAgICAgIH07XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cblxuICB1cGRhdGVGcm9udFBhZ2luYXRpb25EYXRhSWZOZWVkZWQoaXNQYWdlU2l6ZU9yRGF0YUNoYW5nZTogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgbGV0IGRhdGE6IGFueVtdID0gW107IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gICAgaWYgKHRoaXMubnpGcm9udFBhZ2luYXRpb24pIHtcbiAgICAgIHRoaXMubnpUb3RhbCA9IHRoaXMubnpEYXRhLmxlbmd0aDtcbiAgICAgIGlmIChpc1BhZ2VTaXplT3JEYXRhQ2hhbmdlKSB7XG4gICAgICAgIGNvbnN0IG1heFBhZ2VJbmRleCA9IE1hdGguY2VpbCh0aGlzLm56RGF0YS5sZW5ndGggLyB0aGlzLm56UGFnZVNpemUpIHx8IDE7XG4gICAgICAgIGNvbnN0IHBhZ2VJbmRleCA9IHRoaXMubnpQYWdlSW5kZXggPiBtYXhQYWdlSW5kZXggPyBtYXhQYWdlSW5kZXggOiB0aGlzLm56UGFnZUluZGV4O1xuICAgICAgICBpZiAocGFnZUluZGV4ICE9PSB0aGlzLm56UGFnZUluZGV4KSB7XG4gICAgICAgICAgdGhpcy5uelBhZ2VJbmRleCA9IHBhZ2VJbmRleDtcbiAgICAgICAgICBQcm9taXNlLnJlc29sdmUoKS50aGVuKCgpID0+IHRoaXMubnpQYWdlSW5kZXhDaGFuZ2UuZW1pdChwYWdlSW5kZXgpKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgZGF0YSA9IHRoaXMubnpEYXRhLnNsaWNlKCh0aGlzLm56UGFnZUluZGV4IC0gMSkgKiB0aGlzLm56UGFnZVNpemUsIHRoaXMubnpQYWdlSW5kZXggKiB0aGlzLm56UGFnZVNpemUpO1xuICAgIH0gZWxzZSB7XG4gICAgICBkYXRhID0gdGhpcy5uekRhdGE7XG4gICAgfVxuICAgIHRoaXMuZGF0YSA9IFsuLi5kYXRhXTtcbiAgICB0aGlzLm56Q3VycmVudFBhZ2VEYXRhQ2hhbmdlLm5leHQodGhpcy5kYXRhKTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgcmVuZGVyZXI6IFJlbmRlcmVyMixcbiAgICBwcml2YXRlIG5nWm9uZTogTmdab25lLFxuICAgIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBwcml2YXRlIG56TWVhc3VyZVNjcm9sbGJhclNlcnZpY2U6IE56TWVhc3VyZVNjcm9sbGJhclNlcnZpY2UsXG4gICAgcHJpdmF0ZSBpMThuOiBOekkxOG5TZXJ2aWNlLFxuICAgIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWZcbiAgKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LXRhYmxlLXdyYXBwZXInKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuaTE4bi5sb2NhbGVDaGFuZ2UucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95JCkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICB0aGlzLmxvY2FsZSA9IHRoaXMuaTE4bi5nZXRMb2NhbGVEYXRhKCdUYWJsZScpO1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpTY3JvbGwpIHtcbiAgICAgIGlmIChjaGFuZ2VzLm56U2Nyb2xsLmN1cnJlbnRWYWx1ZSkge1xuICAgICAgICB0aGlzLm56U2Nyb2xsID0gY2hhbmdlcy5uelNjcm9sbC5jdXJyZW50VmFsdWU7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLm56U2Nyb2xsID0geyB4OiBudWxsLCB5OiBudWxsIH07XG4gICAgICB9XG4gICAgICB0aGlzLnNldFNjcm9sbFBvc2l0aW9uQ2xhc3NOYW1lKCk7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56UGFnZUluZGV4IHx8IGNoYW5nZXMubnpQYWdlU2l6ZSB8fCBjaGFuZ2VzLm56RnJvbnRQYWdpbmF0aW9uIHx8IGNoYW5nZXMubnpEYXRhKSB7XG4gICAgICB0aGlzLnVwZGF0ZUZyb250UGFnaW5hdGlvbkRhdGFJZk5lZWRlZCghIShjaGFuZ2VzLm56UGFnZVNpemUgfHwgY2hhbmdlcy5uekRhdGEpKTtcbiAgICB9XG4gIH1cblxuICBuZ0FmdGVyVmlld0luaXQoKTogdm9pZCB7XG4gICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLnNldFNjcm9sbFBvc2l0aW9uQ2xhc3NOYW1lKCkpO1xuICAgIHRoaXMubmdab25lLnJ1bk91dHNpZGVBbmd1bGFyKCgpID0+IHtcbiAgICAgIG1lcmdlPE1vdXNlRXZlbnQ+KFxuICAgICAgICB0aGlzLnRhYmxlSGVhZGVyTmF0aXZlRWxlbWVudCA/IGZyb21FdmVudDxNb3VzZUV2ZW50Pih0aGlzLnRhYmxlSGVhZGVyTmF0aXZlRWxlbWVudCwgJ3Njcm9sbCcpIDogRU1QVFksXG4gICAgICAgIHRoaXMubWl4VGFibGVCb2R5TmF0aXZlRWxlbWVudCA/IGZyb21FdmVudDxNb3VzZUV2ZW50Pih0aGlzLm1peFRhYmxlQm9keU5hdGl2ZUVsZW1lbnQsICdzY3JvbGwnKSA6IEVNUFRZXG4gICAgICApXG4gICAgICAgIC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSlcbiAgICAgICAgLnN1YnNjcmliZSgoZGF0YTogTW91c2VFdmVudCkgPT4ge1xuICAgICAgICAgIHRoaXMuc3luY1Njcm9sbFRhYmxlKGRhdGEpO1xuICAgICAgICB9KTtcbiAgICAgIGZyb21FdmVudDxVSUV2ZW50Pih3aW5kb3csICdyZXNpemUnKVxuICAgICAgICAucGlwZShcbiAgICAgICAgICBzdGFydFdpdGgodHJ1ZSksXG4gICAgICAgICAgdGFrZVVudGlsKHRoaXMuZGVzdHJveSQpXG4gICAgICAgIClcbiAgICAgICAgLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICAgICAgdGhpcy5maXRTY3JvbGxCYXIoKTtcbiAgICAgICAgICB0aGlzLnNldFNjcm9sbFBvc2l0aW9uQ2xhc3NOYW1lKCk7XG4gICAgICAgIH0pO1xuICAgIH0pO1xuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMubGlzdE9mTnpUaENvbXBvbmVudC5jaGFuZ2VzXG4gICAgICAucGlwZShcbiAgICAgICAgc3RhcnRXaXRoKHRydWUpLFxuICAgICAgICBmbGF0TWFwKCgpID0+XG4gICAgICAgICAgbWVyZ2UodGhpcy5saXN0T2ZOelRoQ29tcG9uZW50LmNoYW5nZXMsIC4uLnRoaXMubGlzdE9mTnpUaENvbXBvbmVudC5tYXAodGggPT4gdGgubnpXaWR0aENoYW5nZSQpKVxuICAgICAgICApLFxuICAgICAgICB0YWtlVW50aWwodGhpcy5kZXN0cm95JClcbiAgICAgIClcbiAgICAgIC5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICAgIH0pO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=