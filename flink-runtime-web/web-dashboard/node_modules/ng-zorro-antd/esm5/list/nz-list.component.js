/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, Input, TemplateRef, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
var NzListComponent = /** @class */ (function () {
    // #endregion
    function NzListComponent(el, updateHostClassService) {
        this.el = el;
        this.updateHostClassService = updateHostClassService;
        this.nzBordered = false;
        this.nzItemLayout = 'horizontal';
        this.nzLoading = false;
        this.nzSize = 'default';
        this.nzSplit = true;
        // #endregion
        // #region styles
        this.prefixCls = 'ant-list';
    }
    /**
     * @private
     * @return {?}
     */
    NzListComponent.prototype._setClassMap = /**
     * @private
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var classMap = (_a = {},
            _a[this.prefixCls] = true,
            _a[this.prefixCls + "-vertical"] = this.nzItemLayout === 'vertical',
            _a[this.prefixCls + "-lg"] = this.nzSize === 'large',
            _a[this.prefixCls + "-sm"] = this.nzSize === 'small',
            _a[this.prefixCls + "-split"] = this.nzSplit,
            _a[this.prefixCls + "-bordered"] = this.nzBordered,
            _a[this.prefixCls + "-loading"] = this.nzLoading,
            _a[this.prefixCls + "-grid"] = this.nzGrid,
            _a[this.prefixCls + "-something-after-last-item"] = !!(this.nzLoadMore || this.nzPagination || this.nzFooter),
            _a);
        this.updateHostClassService.updateHostClass(this.el.nativeElement, classMap);
    };
    /**
     * @return {?}
     */
    NzListComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this._setClassMap();
    };
    /**
     * @return {?}
     */
    NzListComponent.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this._setClassMap();
    };
    NzListComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-list',
                    template: "<ng-template #itemsTpl>\n  <ng-container *ngFor=\"let item of nzDataSource; let index = index\">\n    <ng-template [ngTemplateOutlet]=\"nzRenderItem\" [ngTemplateOutletContext]=\"{ $implicit: item, index: index }\"></ng-template>\n  </ng-container>\n</ng-template>\n<div *ngIf=\"nzHeader\" class=\"ant-list-header\">\n  <ng-container *nzStringTemplateOutlet=\"nzHeader\">{{ nzHeader }}</ng-container>\n</div>\n<nz-spin [nzSpinning]=\"nzLoading\">\n  <ng-container *ngIf=\"nzDataSource\">\n    <div *ngIf=\"nzLoading && nzDataSource.length === 0\" [style.min-height.px]=\"53\"></div>\n    <div *ngIf=\"nzGrid; else itemsTpl\" nz-row [nzGutter]=\"nzGrid.gutter\">\n      <div nz-col [nzSpan]=\"nzGrid.span\" [nzXs]=\"nzGrid.xs\" [nzSm]=\"nzGrid.sm\" [nzMd]=\"nzGrid.md\" [nzLg]=\"nzGrid.lg\" [nzXl]=\"nzGrid.xl\"\n           [nzXXl]=\"nzGrid.xxl\" *ngFor=\"let item of nzDataSource; let index = index\">\n        <ng-template [ngTemplateOutlet]=\"nzRenderItem\" [ngTemplateOutletContext]=\"{ $implicit: item, index: index }\"></ng-template>\n      </div>\n    </div>\n    <div *ngIf=\"!nzLoading && nzDataSource.length === 0\" class=\"ant-list-empty-text\">\n      <nz-embed-empty [nzComponentName]=\"'list'\" [specificContent]=\"nzNoResult\"></nz-embed-empty>\n    </div>\n  </ng-container>\n  <ng-content></ng-content>\n</nz-spin>\n<div *ngIf=\"nzFooter\" class=\"ant-list-footer\">\n  <ng-container *nzStringTemplateOutlet=\"nzFooter\">{{ nzFooter }}</ng-container>\n</div>\n<ng-template [ngTemplateOutlet]=\"nzLoadMore\"></ng-template>\n<div *ngIf=\"nzPagination\" class=\"ant-list-pagination\">\n  <ng-template [ngTemplateOutlet]=\"nzPagination\"></ng-template>\n</div>",
                    providers: [NzUpdateHostClassService],
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    styles: ["\n      nz-list,\n      nz-list nz-spin {\n        display: block;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzListComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: NzUpdateHostClassService }
    ]; };
    NzListComponent.propDecorators = {
        nzDataSource: [{ type: Input }],
        nzBordered: [{ type: Input }],
        nzGrid: [{ type: Input }],
        nzHeader: [{ type: Input }],
        nzFooter: [{ type: Input }],
        nzItemLayout: [{ type: Input }],
        nzRenderItem: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzLoadMore: [{ type: Input }],
        nzPagination: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzSplit: [{ type: Input }],
        nzNoResult: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzListComponent.prototype, "nzBordered", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzListComponent.prototype, "nzLoading", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzListComponent.prototype, "nzSplit", void 0);
    return NzListComponent;
}());
export { NzListComponent };
if (false) {
    /** @type {?} */
    NzListComponent.prototype.nzDataSource;
    /** @type {?} */
    NzListComponent.prototype.nzBordered;
    /** @type {?} */
    NzListComponent.prototype.nzGrid;
    /** @type {?} */
    NzListComponent.prototype.nzHeader;
    /** @type {?} */
    NzListComponent.prototype.nzFooter;
    /** @type {?} */
    NzListComponent.prototype.nzItemLayout;
    /** @type {?} */
    NzListComponent.prototype.nzRenderItem;
    /** @type {?} */
    NzListComponent.prototype.nzLoading;
    /** @type {?} */
    NzListComponent.prototype.nzLoadMore;
    /** @type {?} */
    NzListComponent.prototype.nzPagination;
    /** @type {?} */
    NzListComponent.prototype.nzSize;
    /** @type {?} */
    NzListComponent.prototype.nzSplit;
    /** @type {?} */
    NzListComponent.prototype.nzNoResult;
    /**
     * @type {?}
     * @private
     */
    NzListComponent.prototype.prefixCls;
    /**
     * @type {?}
     * @private
     */
    NzListComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzListComponent.prototype.updateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbGlzdC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibGlzdC9uei1saXN0LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBR0wsV0FBVyxFQUNYLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSw0Q0FBNEMsQ0FBQztBQUV0RixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFJcEQ7SUFrRUUsYUFBYTtJQUViLHlCQUFvQixFQUFjLEVBQVUsc0JBQWdEO1FBQXhFLE9BQUUsR0FBRixFQUFFLENBQVk7UUFBVSwyQkFBc0IsR0FBdEIsc0JBQXNCLENBQTBCO1FBL0NuRSxlQUFVLEdBQUcsS0FBSyxDQUFDO1FBUW5DLGlCQUFZLEdBQThCLFlBQVksQ0FBQztRQUl2QyxjQUFTLEdBQUcsS0FBSyxDQUFDO1FBTWxDLFdBQU0sR0FBa0IsU0FBUyxDQUFDO1FBRWxCLFlBQU8sR0FBRyxJQUFJLENBQUM7OztRQVFoQyxjQUFTLEdBQUcsVUFBVSxDQUFDO0lBbUJnRSxDQUFDOzs7OztJQWpCeEYsc0NBQVk7Ozs7SUFBcEI7OztZQUNRLFFBQVE7WUFDWixHQUFDLElBQUksQ0FBQyxTQUFTLElBQUcsSUFBSTtZQUN0QixHQUFJLElBQUksQ0FBQyxTQUFTLGNBQVcsSUFBRyxJQUFJLENBQUMsWUFBWSxLQUFLLFVBQVU7WUFDaEUsR0FBSSxJQUFJLENBQUMsU0FBUyxRQUFLLElBQUcsSUFBSSxDQUFDLE1BQU0sS0FBSyxPQUFPO1lBQ2pELEdBQUksSUFBSSxDQUFDLFNBQVMsUUFBSyxJQUFHLElBQUksQ0FBQyxNQUFNLEtBQUssT0FBTztZQUNqRCxHQUFJLElBQUksQ0FBQyxTQUFTLFdBQVEsSUFBRyxJQUFJLENBQUMsT0FBTztZQUN6QyxHQUFJLElBQUksQ0FBQyxTQUFTLGNBQVcsSUFBRyxJQUFJLENBQUMsVUFBVTtZQUMvQyxHQUFJLElBQUksQ0FBQyxTQUFTLGFBQVUsSUFBRyxJQUFJLENBQUMsU0FBUztZQUM3QyxHQUFJLElBQUksQ0FBQyxTQUFTLFVBQU8sSUFBRyxJQUFJLENBQUMsTUFBTTtZQUN2QyxHQUFJLElBQUksQ0FBQyxTQUFTLCtCQUE0QixJQUFHLENBQUMsQ0FBQyxDQUFDLElBQUksQ0FBQyxVQUFVLElBQUksSUFBSSxDQUFDLFlBQVksSUFBSSxJQUFJLENBQUMsUUFBUSxDQUFDO2VBQzNHO1FBQ0QsSUFBSSxDQUFDLHNCQUFzQixDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLGFBQWEsRUFBRSxRQUFRLENBQUMsQ0FBQztJQUMvRSxDQUFDOzs7O0lBTUQsa0NBQVE7OztJQUFSO1FBQ0UsSUFBSSxDQUFDLFlBQVksRUFBRSxDQUFDO0lBQ3RCLENBQUM7Ozs7SUFFRCxxQ0FBVzs7O0lBQVg7UUFDRSxJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDdEIsQ0FBQzs7Z0JBNUVGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsU0FBUztvQkFDbkIsb3BEQUF1QztvQkFDdkMsU0FBUyxFQUFFLENBQUMsd0JBQXdCLENBQUM7b0JBQ3JDLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO29CQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTs2QkFFN0MsbUZBS0M7aUJBRUo7Ozs7Z0JBN0JDLFVBQVU7Z0JBUUgsd0JBQXdCOzs7K0JBeUI5QixLQUFLOzZCQUVMLEtBQUs7eUJBRUwsS0FBSzsyQkFFTCxLQUFLOzJCQUVMLEtBQUs7K0JBRUwsS0FBSzsrQkFFTCxLQUFLOzRCQUVMLEtBQUs7NkJBRUwsS0FBSzsrQkFFTCxLQUFLO3lCQUVMLEtBQUs7MEJBRUwsS0FBSzs2QkFFTCxLQUFLOztJQXRCbUI7UUFBZixZQUFZLEVBQUU7O3VEQUFvQjtJQVluQjtRQUFmLFlBQVksRUFBRTs7c0RBQW1CO0lBUWxCO1FBQWYsWUFBWSxFQUFFOztvREFBZ0I7SUFvQzFDLHNCQUFDO0NBQUEsQUE3RUQsSUE2RUM7U0E3RFksZUFBZTs7O0lBRzFCLHVDQUE2Qjs7SUFFN0IscUNBQTRDOztJQUU1QyxpQ0FBNEI7O0lBRTVCLG1DQUE4Qzs7SUFFOUMsbUNBQThDOztJQUU5Qyx1Q0FBZ0U7O0lBRWhFLHVDQUF5Qzs7SUFFekMsb0NBQTJDOztJQUUzQyxxQ0FBdUM7O0lBRXZDLHVDQUF5Qzs7SUFFekMsaUNBQTJDOztJQUUzQyxrQ0FBd0M7O0lBRXhDLHFDQUFnRDs7Ozs7SUFNaEQsb0NBQStCOzs7OztJQW1CbkIsNkJBQXNCOzs7OztJQUFFLGlEQUF3RCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpTaXplTERTVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5cbmltcG9ydCB7IE56TGlzdEdyaWQgfSBmcm9tICcuL2ludGVyZmFjZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWxpc3QnLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotbGlzdC5jb21wb25lbnQuaHRtbCcsXG4gIHByb3ZpZGVyczogW056VXBkYXRlSG9zdENsYXNzU2VydmljZV0sXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgbnotbGlzdCxcbiAgICAgIG56LWxpc3Qgbnotc3BpbiB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOekxpc3RDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIC8vICNyZWdpb24gZmllbGRzXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgQElucHV0KCkgbnpEYXRhU291cmNlOiBhbnlbXTtcblxuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpCb3JkZXJlZCA9IGZhbHNlO1xuXG4gIEBJbnB1dCgpIG56R3JpZDogTnpMaXN0R3JpZDtcblxuICBASW5wdXQoKSBuekhlYWRlcjogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG5cbiAgQElucHV0KCkgbnpGb290ZXI6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIEBJbnB1dCgpIG56SXRlbUxheW91dDogJ3ZlcnRpY2FsJyB8ICdob3Jpem9udGFsJyA9ICdob3Jpem9udGFsJztcblxuICBASW5wdXQoKSBuelJlbmRlckl0ZW06IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekxvYWRpbmcgPSBmYWxzZTtcblxuICBASW5wdXQoKSBuekxvYWRNb3JlOiBUZW1wbGF0ZVJlZjx2b2lkPjtcblxuICBASW5wdXQoKSBuelBhZ2luYXRpb246IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIEBJbnB1dCgpIG56U2l6ZTogTnpTaXplTERTVHlwZSA9ICdkZWZhdWx0JztcblxuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTcGxpdCA9IHRydWU7XG5cbiAgQElucHV0KCkgbnpOb1Jlc3VsdDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG5cbiAgLy8gI2VuZHJlZ2lvblxuXG4gIC8vICNyZWdpb24gc3R5bGVzXG5cbiAgcHJpdmF0ZSBwcmVmaXhDbHMgPSAnYW50LWxpc3QnO1xuXG4gIHByaXZhdGUgX3NldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIGNvbnN0IGNsYXNzTWFwID0ge1xuICAgICAgW3RoaXMucHJlZml4Q2xzXTogdHJ1ZSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tdmVydGljYWxgXTogdGhpcy5uekl0ZW1MYXlvdXQgPT09ICd2ZXJ0aWNhbCcsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWxnYF06IHRoaXMubnpTaXplID09PSAnbGFyZ2UnLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1zbWBdOiB0aGlzLm56U2l6ZSA9PT0gJ3NtYWxsJyxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tc3BsaXRgXTogdGhpcy5uelNwbGl0LFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1ib3JkZXJlZGBdOiB0aGlzLm56Qm9yZGVyZWQsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWxvYWRpbmdgXTogdGhpcy5uekxvYWRpbmcsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LWdyaWRgXTogdGhpcy5uekdyaWQsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LXNvbWV0aGluZy1hZnRlci1sYXN0LWl0ZW1gXTogISEodGhpcy5uekxvYWRNb3JlIHx8IHRoaXMubnpQYWdpbmF0aW9uIHx8IHRoaXMubnpGb290ZXIpXG4gICAgfTtcbiAgICB0aGlzLnVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWwubmF0aXZlRWxlbWVudCwgY2xhc3NNYXApO1xuICB9XG5cbiAgLy8gI2VuZHJlZ2lvblxuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgZWw6IEVsZW1lbnRSZWYsIHByaXZhdGUgdXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuX3NldENsYXNzTWFwKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcygpOiB2b2lkIHtcbiAgICB0aGlzLl9zZXRDbGFzc01hcCgpO1xuICB9XG59XG4iXX0=