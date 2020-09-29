/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, Host, Input, Optional, Renderer2, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
import { NzColDirective } from '../grid/nz-col.directive';
import { NzRowDirective } from '../grid/nz-row.directive';
import { NzFormItemComponent } from './nz-form-item.component';
var NzFormLabelComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzFormLabelComponent, _super);
    function NzFormLabelComponent(nzUpdateHostClassService, elementRef, nzFormItemComponent, nzRowDirective, renderer) {
        var _this = _super.call(this, nzUpdateHostClassService, elementRef, nzFormItemComponent || nzRowDirective, renderer) || this;
        _this.nzRequired = false;
        renderer.addClass(elementRef.nativeElement, 'ant-form-item-label');
        return _this;
    }
    /**
     * @return {?}
     */
    NzFormLabelComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        _super.prototype.ngOnDestroy.call(this);
    };
    /**
     * @return {?}
     */
    NzFormLabelComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        _super.prototype.ngAfterViewInit.call(this);
    };
    NzFormLabelComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-form-label',
                    providers: [NzUpdateHostClassService],
                    preserveWhitespaces: false,
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    template: "<label [attr.for]=\"nzFor\" [class.ant-form-item-required]=\"nzRequired\">\n  <ng-content></ng-content>\n</label>"
                }] }
    ];
    /** @nocollapse */
    NzFormLabelComponent.ctorParameters = function () { return [
        { type: NzUpdateHostClassService },
        { type: ElementRef },
        { type: NzFormItemComponent, decorators: [{ type: Optional }, { type: Host }] },
        { type: NzRowDirective, decorators: [{ type: Optional }, { type: Host }] },
        { type: Renderer2 }
    ]; };
    NzFormLabelComponent.propDecorators = {
        nzFor: [{ type: Input }],
        nzRequired: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzFormLabelComponent.prototype, "nzRequired", void 0);
    return NzFormLabelComponent;
}(NzColDirective));
export { NzFormLabelComponent };
if (false) {
    /** @type {?} */
    NzFormLabelComponent.prototype.nzFor;
    /** @type {?} */
    NzFormLabelComponent.prototype.nzRequired;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZm9ybS1sYWJlbC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZm9ybS9uei1mb3JtLWxhYmVsLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixJQUFJLEVBQ0osS0FBSyxFQUVMLFFBQVEsRUFDUixTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBQ3RGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDMUQsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQzFELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBRS9EO0lBUTBDLGdEQUFjO0lBSXRELDhCQUNFLHdCQUFrRCxFQUNsRCxVQUFzQixFQUNGLG1CQUF3QyxFQUN4QyxjQUE4QixFQUNsRCxRQUFtQjtRQUxyQixZQU9FLGtCQUFNLHdCQUF3QixFQUFFLFVBQVUsRUFBRSxtQkFBbUIsSUFBSSxjQUFjLEVBQUUsUUFBUSxDQUFDLFNBRTdGO1FBWHdCLGdCQUFVLEdBQUcsS0FBSyxDQUFDO1FBVTFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxxQkFBcUIsQ0FBQyxDQUFDOztJQUNyRSxDQUFDOzs7O0lBRUQsMENBQVc7OztJQUFYO1FBQ0UsaUJBQU0sV0FBVyxXQUFFLENBQUM7SUFDdEIsQ0FBQzs7OztJQUVELDhDQUFlOzs7SUFBZjtRQUNFLGlCQUFNLGVBQWUsV0FBRSxDQUFDO0lBQzFCLENBQUM7O2dCQTdCRixTQUFTLFNBQUM7b0JBQ1QsUUFBUSxFQUFFLGVBQWU7b0JBQ3pCLFNBQVMsRUFBRSxDQUFDLHdCQUF3QixDQUFDO29CQUNyQyxtQkFBbUIsRUFBRSxLQUFLO29CQUMxQixhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07b0JBQy9DLDZIQUE2QztpQkFDOUM7Ozs7Z0JBYlEsd0JBQXdCO2dCQVIvQixVQUFVO2dCQVlILG1CQUFtQix1QkFpQnZCLFFBQVEsWUFBSSxJQUFJO2dCQWxCWixjQUFjLHVCQW1CbEIsUUFBUSxZQUFJLElBQUk7Z0JBekJuQixTQUFTOzs7d0JBa0JSLEtBQUs7NkJBQ0wsS0FBSzs7SUFBbUI7UUFBZixZQUFZLEVBQUU7OzREQUFvQjtJQW9COUMsMkJBQUM7Q0FBQSxBQTlCRCxDQVEwQyxjQUFjLEdBc0J2RDtTQXRCWSxvQkFBb0I7OztJQUMvQixxQ0FBdUI7O0lBQ3ZCLDBDQUE0QyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIEFmdGVyVmlld0luaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE9wdGlvbmFsLFxuICBSZW5kZXJlcjIsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56Q29sRGlyZWN0aXZlIH0gZnJvbSAnLi4vZ3JpZC9uei1jb2wuZGlyZWN0aXZlJztcbmltcG9ydCB7IE56Um93RGlyZWN0aXZlIH0gZnJvbSAnLi4vZ3JpZC9uei1yb3cuZGlyZWN0aXZlJztcbmltcG9ydCB7IE56Rm9ybUl0ZW1Db21wb25lbnQgfSBmcm9tICcuL256LWZvcm0taXRlbS5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1mb3JtLWxhYmVsJyxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotZm9ybS1sYWJlbC5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpGb3JtTGFiZWxDb21wb25lbnQgZXh0ZW5kcyBOekNvbERpcmVjdGl2ZSBpbXBsZW1lbnRzIE9uRGVzdHJveSwgQWZ0ZXJWaWV3SW5pdCB7XG4gIEBJbnB1dCgpIG56Rm9yOiBzdHJpbmc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelJlcXVpcmVkID0gZmFsc2U7XG5cbiAgY29uc3RydWN0b3IoXG4gICAgbnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsXG4gICAgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBAT3B0aW9uYWwoKSBASG9zdCgpIG56Rm9ybUl0ZW1Db21wb25lbnQ6IE56Rm9ybUl0ZW1Db21wb25lbnQsXG4gICAgQE9wdGlvbmFsKCkgQEhvc3QoKSBuelJvd0RpcmVjdGl2ZTogTnpSb3dEaXJlY3RpdmUsXG4gICAgcmVuZGVyZXI6IFJlbmRlcmVyMlxuICApIHtcbiAgICBzdXBlcihuelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsIGVsZW1lbnRSZWYsIG56Rm9ybUl0ZW1Db21wb25lbnQgfHwgbnpSb3dEaXJlY3RpdmUsIHJlbmRlcmVyKTtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtZm9ybS1pdGVtLWxhYmVsJyk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICBzdXBlci5uZ09uRGVzdHJveSgpO1xuICB9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIHN1cGVyLm5nQWZ0ZXJWaWV3SW5pdCgpO1xuICB9XG59XG4iXX0=