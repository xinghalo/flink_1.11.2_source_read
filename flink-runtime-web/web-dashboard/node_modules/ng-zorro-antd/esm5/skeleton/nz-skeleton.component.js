/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, Renderer2, ViewEncapsulation } from '@angular/core';
import { toCssPixel } from '../core/util';
var NzSkeletonComponent = /** @class */ (function () {
    function NzSkeletonComponent(cdr, renderer, elementRef) {
        this.cdr = cdr;
        this.nzActive = false;
        this.nzLoading = true;
        this.nzTitle = true;
        this.nzAvatar = false;
        this.nzParagraph = true;
        this.rowsList = [];
        this.widthList = [];
        renderer.addClass(elementRef.nativeElement, 'ant-skeleton');
    }
    /**
     * @param {?=} value
     * @return {?}
     */
    NzSkeletonComponent.prototype.toCSSUnit = /**
     * @param {?=} value
     * @return {?}
     */
    function (value) {
        if (value === void 0) { value = ''; }
        return toCssPixel(value);
    };
    /**
     * @private
     * @return {?}
     */
    NzSkeletonComponent.prototype.getTitleProps = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var hasAvatar = !!this.nzAvatar;
        /** @type {?} */
        var hasParagraph = !!this.nzParagraph;
        /** @type {?} */
        var width = '';
        if (!hasAvatar && hasParagraph) {
            width = '38%';
        }
        else if (hasAvatar && hasParagraph) {
            width = '50%';
        }
        return tslib_1.__assign({ width: width }, this.getProps(this.nzTitle));
    };
    /**
     * @private
     * @return {?}
     */
    NzSkeletonComponent.prototype.getAvatarProps = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var shape = !!this.nzTitle && !this.nzParagraph ? 'square' : 'circle';
        /** @type {?} */
        var size = 'large';
        return tslib_1.__assign({ shape: shape, size: size }, this.getProps(this.nzAvatar));
    };
    /**
     * @private
     * @return {?}
     */
    NzSkeletonComponent.prototype.getParagraphProps = /**
     * @private
     * @return {?}
     */
    function () {
        /** @type {?} */
        var hasAvatar = !!this.nzAvatar;
        /** @type {?} */
        var hasTitle = !!this.nzTitle;
        /** @type {?} */
        var basicProps = {};
        // Width
        if (!hasAvatar || !hasTitle) {
            basicProps.width = '61%';
        }
        // Rows
        if (!hasAvatar && hasTitle) {
            basicProps.rows = 3;
        }
        else {
            basicProps.rows = 2;
        }
        return tslib_1.__assign({}, basicProps, this.getProps(this.nzParagraph));
    };
    /**
     * @private
     * @template T
     * @param {?} prop
     * @return {?}
     */
    NzSkeletonComponent.prototype.getProps = /**
     * @private
     * @template T
     * @param {?} prop
     * @return {?}
     */
    function (prop) {
        return prop && typeof prop === 'object' ? prop : {};
    };
    /**
     * @private
     * @return {?}
     */
    NzSkeletonComponent.prototype.getWidthList = /**
     * @private
     * @return {?}
     */
    function () {
        var _a = this.paragraph, width = _a.width, rows = _a.rows;
        /** @type {?} */
        var widthList = [];
        if (width && Array.isArray(width)) {
            widthList = width;
        }
        else if (width && !Array.isArray(width)) {
            widthList = [];
            widthList[(/** @type {?} */ (rows)) - 1] = width;
        }
        return widthList;
    };
    /**
     * @private
     * @return {?}
     */
    NzSkeletonComponent.prototype.updateProps = /**
     * @private
     * @return {?}
     */
    function () {
        this.title = this.getTitleProps();
        this.avatar = this.getAvatarProps();
        this.paragraph = this.getParagraphProps();
        this.rowsList = tslib_1.__spread(Array(this.paragraph.rows));
        this.widthList = this.getWidthList();
        this.cdr.markForCheck();
    };
    /**
     * @return {?}
     */
    NzSkeletonComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.updateProps();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzSkeletonComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzTitle || changes.nzAvatar || changes.nzParagraph) {
            this.updateProps();
        }
    };
    NzSkeletonComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-skeleton',
                    template: "<ng-container *ngIf=\"nzLoading\">\n  <div class=\"ant-skeleton-header\">\n    <span\n      *ngIf=\"!!nzAvatar\"\n      class=\"ant-skeleton-avatar\"\n      [class.ant-skeleton-avatar-lg]=\"avatar.size === 'large'\"\n      [class.ant-skeleton-avatar-sm]=\"avatar.size === 'small'\"\n      [class.ant-skeleton-avatar-circle]=\"avatar.shape === 'circle'\"\n      [class.ant-skeleton-avatar-square]=\"avatar.shape === 'square'\">\n    </span>\n  </div>\n  <div class=\"ant-skeleton-content\">\n    <h3 *ngIf=\"!!nzTitle\" class=\"ant-skeleton-title\" [style.width]=\"toCSSUnit(title.width)\"></h3>\n    <ul *ngIf=\"!!nzParagraph\" class=\"ant-skeleton-paragraph\">\n      <li *ngFor=\"let row of rowsList; let i=index\" [style.width]=\"toCSSUnit(widthList[i])\">\n      </li>\n    </ul>\n  </div>\n</ng-container>\n<ng-container *ngIf=\"!nzLoading\">\n  <ng-content></ng-content>\n</ng-container>",
                    host: {
                        '[class.ant-skeleton-with-avatar]': '!!nzAvatar',
                        '[class.ant-skeleton-active]': 'nzActive'
                    }
                }] }
    ];
    /** @nocollapse */
    NzSkeletonComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef },
        { type: Renderer2 },
        { type: ElementRef }
    ]; };
    NzSkeletonComponent.propDecorators = {
        nzActive: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzTitle: [{ type: Input }],
        nzAvatar: [{ type: Input }],
        nzParagraph: [{ type: Input }]
    };
    return NzSkeletonComponent;
}());
export { NzSkeletonComponent };
if (false) {
    /** @type {?} */
    NzSkeletonComponent.prototype.nzActive;
    /** @type {?} */
    NzSkeletonComponent.prototype.nzLoading;
    /** @type {?} */
    NzSkeletonComponent.prototype.nzTitle;
    /** @type {?} */
    NzSkeletonComponent.prototype.nzAvatar;
    /** @type {?} */
    NzSkeletonComponent.prototype.nzParagraph;
    /** @type {?} */
    NzSkeletonComponent.prototype.title;
    /** @type {?} */
    NzSkeletonComponent.prototype.avatar;
    /** @type {?} */
    NzSkeletonComponent.prototype.paragraph;
    /** @type {?} */
    NzSkeletonComponent.prototype.rowsList;
    /** @type {?} */
    NzSkeletonComponent.prototype.widthList;
    /**
     * @type {?}
     * @private
     */
    NzSkeletonComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2tlbGV0b24uY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInNrZWxldG9uL256LXNrZWxldG9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsS0FBSyxFQUdMLFNBQVMsRUFFVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLFVBQVUsRUFBRSxNQUFNLGNBQWMsQ0FBQztBQUcxQztJQXVCRSw2QkFBb0IsR0FBc0IsRUFBRSxRQUFtQixFQUFFLFVBQXNCO1FBQW5FLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBWmpDLGFBQVEsR0FBRyxLQUFLLENBQUM7UUFDakIsY0FBUyxHQUFHLElBQUksQ0FBQztRQUNqQixZQUFPLEdBQThCLElBQUksQ0FBQztRQUMxQyxhQUFRLEdBQStCLEtBQUssQ0FBQztRQUM3QyxnQkFBVyxHQUFrQyxJQUFJLENBQUM7UUFLM0QsYUFBUSxHQUFhLEVBQUUsQ0FBQztRQUN4QixjQUFTLEdBQTJCLEVBQUUsQ0FBQztRQUdyQyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsY0FBYyxDQUFDLENBQUM7SUFDOUQsQ0FBQzs7Ozs7SUFFRCx1Q0FBUzs7OztJQUFULFVBQVUsS0FBMkI7UUFBM0Isc0JBQUEsRUFBQSxVQUEyQjtRQUNuQyxPQUFPLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUMzQixDQUFDOzs7OztJQUVPLDJDQUFhOzs7O0lBQXJCOztZQUNRLFNBQVMsR0FBWSxDQUFDLENBQUMsSUFBSSxDQUFDLFFBQVE7O1lBQ3BDLFlBQVksR0FBWSxDQUFDLENBQUMsSUFBSSxDQUFDLFdBQVc7O1lBQzVDLEtBQUssR0FBRyxFQUFFO1FBQ2QsSUFBSSxDQUFDLFNBQVMsSUFBSSxZ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OztJQUFYLFVBQVksT0FBc0I7UUFDaEMsSUFBSSxPQUFPLENBQUMsT0FBTyxJQUFJLE9BQU8sQ0FBQyxRQUFRLElBQUksT0FBTyxDQUFDLFdBQVcsRUFBRTtZQUM5RCxJQUFJLENBQUMsV0FBVyxFQUFFLENBQUM7U0FDcEI7SUFDSCxDQUFDOztnQkFuR0YsU0FBUyxTQUFDO29CQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsUUFBUSxFQUFFLGFBQWE7b0JBQ3ZCLHk0QkFBMkM7b0JBQzNDLElBQUksRUFBRTt3QkFDSixrQ0FBa0MsRUFBRSxZQUFZO3dCQUNoRCw2QkFBNkIsRUFBRSxVQUFVO3FCQUMxQztpQkFDRjs7OztnQkF2QkMsaUJBQWlCO2dCQU1qQixTQUFTO2dCQUpULFVBQVU7OzsyQkF1QlQsS0FBSzs0QkFDTCxLQUFLOzBCQUNMLEtBQUs7MkJBQ0wsS0FBSzs4QkFDTCxLQUFLOztJQXFGUiwwQkFBQztDQUFBLEFBcEdELElBb0dDO1NBMUZZLG1CQUFtQjs7O0lBQzlCLHVDQUEwQjs7SUFDMUIsd0NBQTBCOztJQUMxQixzQ0FBbUQ7O0lBQ25ELHVDQUFzRDs7SUFDdEQsMENBQTJEOztJQUUzRCxvQ0FBdUI7O0lBQ3ZCLHFDQUF5Qjs7SUFDekIsd0NBQStCOztJQUMvQix1Q0FBd0I7O0lBQ3hCLHdDQUF1Qzs7Ozs7SUFFM0Isa0NBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgUmVuZGVyZXIyLFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgdG9Dc3NQaXhlbCB9IGZyb20gJy4uL2NvcmUvdXRpbCc7XG5pbXBvcnQgeyBBdmF0YXJTaGFwZSwgQXZhdGFyU2l6ZSwgTnpTa2VsZXRvbkF2YXRhciwgTnpTa2VsZXRvblBhcmFncmFwaCwgTnpTa2VsZXRvblRpdGxlIH0gZnJvbSAnLi9uei1za2VsZXRvbi50eXBlJztcblxuQENvbXBvbmVudCh7XG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBzZWxlY3RvcjogJ256LXNrZWxldG9uJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXNrZWxldG9uLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LXNrZWxldG9uLXdpdGgtYXZhdGFyXSc6ICchIW56QXZhdGFyJyxcbiAgICAnW2NsYXNzLmFudC1za2VsZXRvbi1hY3RpdmVdJzogJ256QWN0aXZlJ1xuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56U2tlbGV0b25Db21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIG56QWN0aXZlID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56TG9hZGluZyA9IHRydWU7XG4gIEBJbnB1dCgpIG56VGl0bGU6IE56U2tlbGV0b25UaXRsZSB8IGJvb2xlYW4gPSB0cnVlO1xuICBASW5wdXQoKSBuekF2YXRhcjogTnpTa2VsZXRvbkF2YXRhciB8IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgbnpQYXJhZ3JhcGg6IE56U2tlbGV0b25QYXJhZ3JhcGggfCBib29sZWFuID0gdHJ1ZTtcblxuICB0aXRsZTogTnpTa2VsZXRvblRpdGxlO1xuICBhdmF0YXI6IE56U2tlbGV0b25BdmF0YXI7XG4gIHBhcmFncmFwaDogTnpTa2VsZXRvblBhcmFncmFwaDtcbiAgcm93c0xpc3Q6IG51bWJlcltdID0gW107XG4gIHdpZHRoTGlzdDogQXJyYXk8bnVtYmVyIHwgc3RyaW5nPiA9IFtdO1xuXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZiwgcmVuZGVyZXI6IFJlbmRlcmVyMiwgZWxlbWVudFJlZjogRWxlbWVudFJlZikge1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1za2VsZXRvbicpO1xuICB9XG5cbiAgdG9DU1NVbml0KHZhbHVlOiBudW1iZXIgfCBzdHJpbmcgPSAnJyk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRvQ3NzUGl4ZWwodmFsdWUpO1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRUaXRsZVByb3BzKCk6IE56U2tlbGV0b25UaXRsZSB7XG4gICAgY29uc3QgaGFzQXZhdGFyOiBib29sZWFuID0gISF0aGlzLm56QXZhdGFyO1xuICAgIGNvbnN0IGhhc1BhcmFncmFwaDogYm9vbGVhbiA9ICEhdGhpcy5uelBhcmFncmFwaDtcbiAgICBsZXQgd2lkdGggPSAnJztcbiAgICBpZiAoIWhhc0F2YXRhciAmJiBoYXNQYXJhZ3JhcGgpIHtcbiAgICAgIHdpZHRoID0gJzM4JSc7XG4gICAgfSBlbHNlIGlmIChoYXNBdmF0YXIgJiYgaGFzUGFyYWdyYXBoKSB7XG4gICAgICB3aWR0aCA9ICc1MCUnO1xuICAgIH1cbiAgICByZXR1cm4geyB3aWR0aCwgLi4udGhpcy5nZXRQcm9wcyh0aGlzLm56VGl0bGUpIH07XG4gIH1cblxuICBwcml2YXRlIGdldEF2YXRhclByb3BzKCk6IE56U2tlbGV0b25BdmF0YXIge1xuICAgIGNvbnN0IHNoYXBlOiBBdmF0YXJTaGFwZSA9ICEhdGhpcy5uelRpdGxlICYmICF0aGlzLm56UGFyYWdyYXBoID8gJ3NxdWFyZScgOiAnY2lyY2xlJztcbiAgICBjb25zdCBzaXplOiBBdmF0YXJTaXplID0gJ2xhcmdlJztcbiAgICByZXR1cm4geyBzaGFwZSwgc2l6ZSwgLi4udGhpcy5nZXRQcm9wcyh0aGlzLm56QXZhdGFyKSB9O1xuICB9XG5cbiAgcHJpdmF0ZSBnZXRQYXJhZ3JhcGhQcm9wcygpOiBOelNrZWxldG9uUGFyYWdyYXBoIHtcbiAgICBjb25zdCBoYXNBdmF0YXI6IGJvb2xlYW4gPSAhIXRoaXMubnpBdmF0YXI7XG4gICAgY29uc3QgaGFzVGl0bGU6IGJvb2xlYW4gPSAhIXRoaXMubnpUaXRsZTtcbiAgICBjb25zdCBiYXNpY1Byb3BzOiBOelNrZWxldG9uUGFyYWdyYXBoID0ge307XG4gICAgLy8gV2lkdGhcbiAgICBpZiAoIWhhc0F2YXRhciB8fCAhaGFzVGl0bGUpIHtcbiAgICAgIGJhc2ljUHJvcHMud2lkdGggPSAnNjElJztcbiAgICB9XG4gICAgLy8gUm93c1xuICAgIGlmICghaGFzQXZhdGFyICYmIGhhc1RpdGxlKSB7XG4gICAgICBiYXNpY1Byb3BzLnJvd3MgPSAzO1xuICAgIH0gZWxzZSB7XG4gICAgICBiYXNpY1Byb3BzLnJvd3MgPSAyO1xuICAgIH1cbiAgICByZXR1cm4geyAuLi5iYXNpY1Byb3BzLCAuLi50aGlzLmdldFByb3BzKHRoaXMubnpQYXJhZ3JhcGgpIH07XG4gIH1cblxuICBwcml2YXRlIGdldFByb3BzPFQ+KHByb3A6IFQgfCBib29sZWFuIHwgdW5kZWZpbmVkKTogVCB8IHt9IHtcbiAgICByZXR1cm4gcHJvcCAmJiB0eXBlb2YgcHJvcCA9PT0gJ29iamVjdCcgPyBwcm9wIDoge307XG4gIH1cblxuICBwcml2YXRlIGdldFdpZHRoTGlzdCgpOiBBcnJheTxudW1iZXIgfCBzdHJpbmc+IHtcbiAgICBjb25zdCB7IHdpZHRoLCByb3dzIH0gPSB0aGlzLnBhcmFncmFwaDtcbiAgICBsZXQgd2lkdGhMaXN0OiBBcnJheTxzdHJpbmcgfCBudW1iZXI+ID0gW107XG4gICAgaWYgKHdpZHRoICYmIEFycmF5LmlzQXJyYXkod2lkdGgpKSB7XG4gICAgICB3aWR0aExpc3QgPSB3aWR0aDtcbiAgICB9IGVsc2UgaWYgKHdpZHRoICYmICFBcnJheS5pc0FycmF5KHdpZHRoKSkge1xuICAgICAgd2lkdGhMaXN0ID0gW107XG4gICAgICB3aWR0aExpc3Rbcm93cyEgLSAxXSA9IHdpZHRoO1xuICAgIH1cbiAgICByZXR1cm4gd2lkdGhMaXN0O1xuICB9XG5cbiAgcHJpdmF0ZSB1cGRhdGVQcm9wcygpOiB2b2lkIHtcbiAgICB0aGlzLnRpdGxlID0gdGhpcy5nZXRUaXRsZVByb3BzKCk7XG4gICAgdGhpcy5hdmF0YXIgPSB0aGlzLmdldEF2YXRhclByb3BzKCk7XG4gICAgdGhpcy5wYXJhZ3JhcGggPSB0aGlzLmdldFBhcmFncmFwaFByb3BzKCk7XG4gICAgdGhpcy5yb3dzTGlzdCA9IFsuLi5BcnJheSh0aGlzLnBhcmFncmFwaC5yb3dzKV07XG4gICAgdGhpcy53aWR0aExpc3QgPSB0aGlzLmdldFdpZHRoTGlzdCgpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVQcm9wcygpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLm56VGl0bGUgfHwgY2hhbmdlcy5uekF2YXRhciB8fCBjaGFuZ2VzLm56UGFyYWdyYXBoKSB7XG4gICAgICB0aGlzLnVwZGF0ZVByb3BzKCk7XG4gICAgfVxuICB9XG59XG4iXX0=