/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, Input, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util';
export class NzDividerComponent {
    /**
     * @param {?} elementRef
     * @param {?} nzUpdateHostClassService
     */
    constructor(elementRef, nzUpdateHostClassService) {
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
    setClass() {
        /** @type {?} */
        const orientationPrefix = this.nzOrientation.length > 0 ? '-' + this.nzOrientation : this.nzOrientation;
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, {
            ['ant-divider']: true,
            [`ant-divider-${this.nzType}`]: true,
            [`ant-divider-with-text${orientationPrefix}`]: this.nzText,
            [`ant-divider-dashed`]: this.nzDashed
        });
    }
    /**
     * @return {?}
     */
    ngOnChanges() {
        this.setClass();
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.setClass();
    }
}
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
NzDividerComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: NzUpdateHostClassService }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZGl2aWRlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZGl2aWRlci9uei1kaXZpZGVyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBSUwsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBQ3RGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxjQUFjLENBQUM7QUFVNUMsTUFBTSxPQUFPLGtCQUFrQjs7Ozs7SUFnQjdCLFlBQW9CLFVBQXNCLEVBQVUsd0JBQWtEO1FBQWxGLGVBQVUsR0FBVixVQUFVLENBQVk7UUFBVSw2QkFBd0IsR0FBeEIsd0JBQXdCLENBQTBCO1FBZDdGLFdBQU0sR0FBOEIsWUFBWSxDQUFDO1FBQ2pELGtCQUFhLEdBQTBCLEVBQUUsQ0FBQztRQUMxQixhQUFRLEdBQUcsS0FBSyxDQUFDO0lBWStELENBQUM7Ozs7O0lBVmxHLFFBQVE7O2NBQ1IsaUJBQWlCLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxNQUFNLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxHQUFHLEdBQUcsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDLENBQUMsSUFBSSxDQUFDLGFBQWE7UUFDdkcsSUFBSSxDQUFDLHdCQUF3QixDQUFDLGVBQWUsQ0FBQyxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRTtZQUMzRSxDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUk7WUFDckIsQ0FBQyxlQUFlLElBQUksQ0FBQyxNQUFNLEVBQUUsQ0FBQyxFQUFFLElBQUk7WUFDcEMsQ0FBQyx3QkFBd0IsaUJBQWlCLEVBQUUsQ0FBQyxFQUFFLElBQUksQ0FBQyxNQUFNO1lBQzFELENBQUMsb0JBQW9CLENBQUMsRUFBRSxJQUFJLENBQUMsUUFBUTtTQUN0QyxDQUFDLENBQUM7SUFDTCxDQUFDOzs7O0lBSUQsV0FBVztRQUNULElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztJQUNsQixDQUFDOzs7O0lBRUQsUUFBUTtRQUNOLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQztJQUNsQixDQUFDOzs7WUFoQ0YsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxZQUFZO2dCQUN0Qiw2SkFBMEM7Z0JBQzFDLG1CQUFtQixFQUFFLEtBQUs7Z0JBQzFCLFNBQVMsRUFBRSxDQUFDLHdCQUF3QixDQUFDO2dCQUNyQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07YUFDaEQ7Ozs7WUFsQkMsVUFBVTtZQVFILHdCQUF3Qjs7O3FCQVk5QixLQUFLO3FCQUNMLEtBQUs7NEJBQ0wsS0FBSzt1QkFDTCxLQUFLOztBQUFtQjtJQUFmLFlBQVksRUFBRTs7b0RBQWtCOzs7SUFIMUMsb0NBQTRDOztJQUM1QyxvQ0FBMEQ7O0lBQzFELDJDQUFtRDs7SUFDbkQsc0NBQTBDOzs7OztJQVk5Qix3Q0FBOEI7Ozs7O0lBQUUsc0RBQTBEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25Jbml0LFxuICBUZW1wbGF0ZVJlZixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSB9IGZyb20gJy4uL2NvcmUvc2VydmljZXMvdXBkYXRlLWhvc3QtY2xhc3Muc2VydmljZSc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1kaXZpZGVyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWRpdmlkZXIuY29tcG9uZW50Lmh0bWwnLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2hcbn0pXG5leHBvcnQgY2xhc3MgTnpEaXZpZGVyQ29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzLCBPbkluaXQge1xuICBASW5wdXQoKSBuelRleHQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuelR5cGU6ICdob3Jpem9udGFsJyB8ICd2ZXJ0aWNhbCcgPSAnaG9yaXpvbnRhbCc7XG4gIEBJbnB1dCgpIG56T3JpZW50YXRpb246ICdsZWZ0JyB8ICdyaWdodCcgfCAnJyA9ICcnO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEYXNoZWQgPSBmYWxzZTtcblxuICBwcml2YXRlIHNldENsYXNzKCk6IHZvaWQge1xuICAgIGNvbnN0IG9yaWVudGF0aW9uUHJlZml4ID0gdGhpcy5uek9yaWVudGF0aW9uLmxlbmd0aCA+IDAgPyAnLScgKyB0aGlzLm56T3JpZW50YXRpb24gOiB0aGlzLm56T3JpZW50YXRpb247XG4gICAgdGhpcy5uelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCB7XG4gICAgICBbJ2FudC1kaXZpZGVyJ106IHRydWUsXG4gICAgICBbYGFudC1kaXZpZGVyLSR7dGhpcy5uelR5cGV9YF06IHRydWUsXG4gICAgICBbYGFudC1kaXZpZGVyLXdpdGgtdGV4dCR7b3JpZW50YXRpb25QcmVmaXh9YF06IHRoaXMubnpUZXh0LFxuICAgICAgW2BhbnQtZGl2aWRlci1kYXNoZWRgXTogdGhpcy5uekRhc2hlZFxuICAgIH0pO1xuICB9XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBlbGVtZW50UmVmOiBFbGVtZW50UmVmLCBwcml2YXRlIG56VXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlKSB7fVxuXG4gIG5nT25DaGFuZ2VzKCk6IHZvaWQge1xuICAgIHRoaXMuc2V0Q2xhc3MoKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHRoaXMuc2V0Q2xhc3MoKTtcbiAgfVxufVxuIl19