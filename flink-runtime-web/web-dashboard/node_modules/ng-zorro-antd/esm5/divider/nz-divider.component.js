/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, Input, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util';
var NzDividerComponent = /** @class */ (function () {
    function NzDividerComponent(elementRef, nzUpdateHostClassService) {
        this.elementRef = elementRef;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.nzType = 'horizontal';
        this.nzOrientation = '';
        this.nzDashed = false;
    }
    /**
     * @private
     * @return {?}
     */
    NzDividerComponent.prototype.setClass = /**
     * @private
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var orientationPrefix = this.nzOrientation.length > 0 ? '-' + this.nzOrientation : this.nzOrientation;
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, (_a = {},
            _a['ant-divider'] = true,
            _a["ant-divider-" + this.nzType] = true,
            _a["ant-divider-with-text" + orientationPrefix] = this.nzText,
            _a["ant-divider-dashed"] = this.nzDashed,
            _a));
    };
    /**
     * @return {?}
     */
    NzDividerComponent.prototype.ngOnChanges = /**
     * @return {?}
     */
    function () {
        this.setClass();
    };
    /**
     * @return {?}
     */
    NzDividerComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setClass();
    };
    NzDividerComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-divider',
                    template: "<span *ngIf=\"nzText\" class=\"ant-divider-inner-text\">\n  <ng-container *nzStringTemplateOutlet=\"nzText\">{{ nzText }}</ng-container>\n</span>",
                    preserveWhitespaces: false,
                    providers: [NzUpdateHostClassService],
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush
                }] }
    ];
    /** @nocollapse */
    NzDividerComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: NzUpdateHostClassService }
    ]; };
    NzDividerComponent.propDecorators = {
        nzText: [{ type: Input }],
        nzType: [{ type: Input }],
        nzOrientation: [{ type: Input }],
        nzDashed: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzDividerComponent.prototype, "nzDashed", void 0);
    return NzDividerComponent;
}());
export { NzDividerComponent };
if (false) {
    /** @type {?} */
    NzDividerComponent.prototype.nzText;
    /** @type {?} */
    NzDividerComponent.prototype.nzType;
    /** @type {?} */
    NzDividerComponent.prototype.nzOrientation;
    /** @type {?} */
    NzDividerComponent.prototype.nzDashed;
    /**
     * @type {?}
     * @private
     */
    NzDividerComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzDividerComponent.prototype.nzUpdateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZGl2aWRlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZGl2aWRlci9uei1kaXZpZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBSUwsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBQ3RGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxjQUFjLENBQUM7QUFFNUM7SUF3QkUsNEJBQW9CLFVBQXNCLEVBQVUsd0JBQWtEO1FBQWxGLGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSw2QkFBd0IsR0FBeEIsd0JBQXdCLENBQTBCO1FBZDdGLFdBQU0sR0FBOEIsWUFBWSxDQUFDO1FBQ2pELGtCQUFhLEdBQTBCLEVBQUUsQ0FBQztRQUMxQixhQUFRLEdBQUcsS0FBSyxDQUFDO0lBWStELENBQUM7Ozs7O0lBVmxHLHFDQUFROzs7O0lBQWhCOzs7WUFDUSxpQkFBaUIsR0FBRyxJQUFJLENBQUMsYUFBYSxDQUFDLE1BQU0sR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLEdBQUcsR0FBRyxJQUFJLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsYUFBYTtRQUN2RyxJQUFJLENBQUMsd0JBQXdCLENBQUMsZUFBZSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYTtZQUN6RSxHQUFDLGFBQWEsSUFBRyxJQUFJO1lBQ3JCLEdBQUMsaUJBQWUsSUFBSSxDQUFDLE1BQVEsSUFBRyxJQUFJO1lBQ3BDLEdBQUMsMEJBQXdCLGlCQUFtQixJQUFHLElBQUksQ0FBQyxNQUFNO1lBQzFELEdBQUMsb0JBQW9CLElBQUcsSUFBSSxDQUFDLFFBQVE7Z0JBQ3JDLENBQUM7SUFDTCxDQUFDOzs7O0lBSUQsd0NBQVc7OztJQUFYO1FBQ0UsSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDO0lBQ2xCLENBQUM7Ozs7SUFFRCxxQ0FBUTs7O0lBQVI7UUFDRSxJQUFJLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDbEIsQ0FBQzs7Z0JBaENGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsWUFBWTtvQkFDdEIsNkpBQTBDO29CQUMxQyxtQkFBbUIsRUFBRSxLQUFLO29CQUMxQixTQUFTLEVBQUUsQ0FBQyx3QkFBd0IsQ0FBQztvQkFDckMsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2lCQUNoRDs7OztnQkFsQkMsVUFBVTtnQkFRSCx3QkFBd0I7Ozt5QkFZOUIsS0FBSzt5QkFDTCxLQUFLO2dDQUNMLEtBQUs7MkJBQ0wsS0FBSzs7SUFBbUI7UUFBZixZQUFZLEVBQUU7O3dEQUFrQjtJQXFCNUMseUJBQUM7Q0FBQSxBQWpDRCxJQWlDQztTQXpCWSxrQkFBa0I7OztJQUM3QixvQ0FBNEM7O0lBQzVDLG9DQUEwRDs7SUFDMUQsMkNBQW1EOztJQUNuRCxzQ0FBMEM7Ozs7O0lBWTlCLHdDQUE4Qjs7Ozs7SUFBRSxzREFBMEQiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkluaXQsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbCc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWRpdmlkZXInLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotZGl2aWRlci5jb21wb25lbnQuaHRtbCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBwcm92aWRlcnM6IFtOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2VdLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaFxufSlcbmV4cG9ydCBjbGFzcyBOekRpdmlkZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMsIE9uSW5pdCB7XG4gIEBJbnB1dCgpIG56VGV4dDogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56VHlwZTogJ2hvcml6b250YWwnIHwgJ3ZlcnRpY2FsJyA9ICdob3Jpem9udGFsJztcbiAgQElucHV0KCkgbnpPcmllbnRhdGlvbjogJ2xlZnQnIHwgJ3JpZ2h0JyB8ICcnID0gJyc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRhc2hlZCA9IGZhbHNlO1xuXG4gIHByaXZhdGUgc2V0Q2xhc3MoKTogdm9pZCB7XG4gICAgY29uc3Qgb3JpZW50YXRpb25QcmVmaXggPSB0aGlzLm56T3JpZW50YXRpb24ubGVuZ3RoID4gMCA/ICctJyArIHRoaXMubnpPcmllbnRhdGlvbiA6IHRoaXMubnpPcmllbnRhdGlvbjtcbiAgICB0aGlzLm56VXBkYXRlSG9zdENsYXNzU2VydmljZS51cGRhdGVIb3N0Q2xhc3ModGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsIHtcbiAgICAgIFsnYW50LWRpdmlkZXInXTogdHJ1ZSxcbiAgICAgIFtgYW50LWRpdmlkZXItJHt0aGlzLm56VHlwZX1gXTogdHJ1ZSxcbiAgICAgIFtgYW50LWRpdmlkZXItd2l0aC10ZXh0JHtvcmllbnRhdGlvblByZWZpeH1gXTogdGhpcy5uelRleHQsXG4gICAgICBbYGFudC1kaXZpZGVyLWRhc2hlZGBdOiB0aGlzLm56RGFzaGVkXG4gICAgfSk7XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsIHByaXZhdGUgbnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UpIHt9XG5cbiAgbmdPbkNoYW5nZXMoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzcygpO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzcygpO1xuICB9XG59XG4iXX0=