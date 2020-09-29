/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ViewEncapsulation } from '@angular/core';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { slideMotion } from '../core/animation/slide';
import { NzMenuDropdownService } from './nz-menu-dropdown.service';
var NzDropdownContextComponent = /** @class */ (function () {
    function NzDropdownContextComponent(cdr) {
        this.cdr = cdr;
        this.open = true;
        this.dropDownPosition = 'bottom';
        this.destroy$ = new Subject();
    }
    /**
     * @param {?} open
     * @param {?} templateRef
     * @param {?} positionChanges
     * @param {?} control
     * @return {?}
     */
    NzDropdownContextComponent.prototype.init = /**
     * @param {?} open
     * @param {?} templateRef
     * @param {?} positionChanges
     * @param {?} control
     * @return {?}
     */
    function (open, templateRef, positionChanges, control) {
        var _this = this;
        this.open = open;
        this.templateRef = templateRef;
        this.control = control;
        positionChanges.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} data
         * @return {?}
         */
        function (data) {
            _this.dropDownPosition = data.connectionPair.overlayY === 'bottom' ? 'top' : 'bottom';
            _this.cdr.markForCheck();
        }));
    };
    /**
     * @return {?}
     */
    NzDropdownContextComponent.prototype.close = /**
     * @return {?}
     */
    function () {
        this.open = false;
        this.cdr.markForCheck();
    };
    /**
     * @return {?}
     */
    NzDropdownContextComponent.prototype.afterAnimation = /**
     * @return {?}
     */
    function () {
        if (!this.open) {
            this.control.dispose();
        }
    };
    // TODO auto set dropdown class after the bug resolved
    /** https://github.com/angular/angular/issues/14842 **/
    // TODO auto set dropdown class after the bug resolved
    /**
     * https://github.com/angular/angular/issues/14842 *
     * @return {?}
     */
    NzDropdownContextComponent.prototype.ngOnDestroy = 
    // TODO auto set dropdown class after the bug resolved
    /**
     * https://github.com/angular/angular/issues/14842 *
     * @return {?}
     */
    function () {
        this.destroy$.next();
        this.destroy$.complete();
    };
    NzDropdownContextComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-dropdown-context',
                    animations: [slideMotion],
                    preserveWhitespaces: false,
                    template: "<div *ngIf=\"open\"\n  class=\"ant-dropdown ant-dropdown-placement-bottomLeft\"\n  [@slideMotion]=\"dropDownPosition\"\n  (@slideMotion.done)=\"afterAnimation()\">\n  <ng-template [ngTemplateOutlet]=\"templateRef\"></ng-template>\n</div>",
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    providers: [NzMenuDropdownService],
                    styles: ["\n      nz-dropdown-context {\n        display: block;\n      }\n\n      .ant-dropdown {\n        top: 100%;\n        left: 0;\n        position: relative;\n        width: 100%;\n        margin-top: 4px;\n        margin-bottom: 4px;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzDropdownContextComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef }
    ]; };
    return NzDropdownContextComponent;
}());
export { NzDropdownContextComponent };
if (false) {
    /** @type {?} */
    NzDropdownContextComponent.prototype.open;
    /** @type {?} */
    NzDropdownContextComponent.prototype.templateRef;
    /** @type {?} */
    NzDropdownContextComponent.prototype.dropDownPosition;
    /**
     * @type {?}
     * @private
     */
    NzDropdownContextComponent.prototype.control;
    /**
     * @type {?}
     * @private
     */
    NzDropdownContextComponent.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzDropdownContextComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZHJvcGRvd24tY29udGV4dC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZHJvcGRvd24vbnotZHJvcGRvd24tY29udGV4dC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUNBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFHVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFjLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUMzQyxPQUFPLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFDM0MsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBRXRELE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBRW5FO0lBMERFLG9DQUFvQixHQUFzQjtRQUF0QixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQWhDMUMsU0FBSSxHQUFHLElBQUksQ0FBQztRQUVaLHFCQUFnQixHQUFxQixRQUFRLENBQUM7UUFFdEMsYUFBUSxHQUFHLElBQUksT0FBTyxFQUFFLENBQUM7SUE0QlksQ0FBQzs7Ozs7Ozs7SUExQjlDLHlDQUFJOzs7Ozs7O0lBQUosVUFDRSxJQUFhLEVBQ2IsV0FBOEIsRUFDOUIsZUFBMkQsRUFDM0QsT0FBMEI7UUFKNUIsaUJBYUM7UUFQQyxJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQztRQUNqQixJQUFJLENBQUMsV0FBVyxHQUFHLFdBQVcsQ0FBQztRQUMvQixJQUFJLENBQUMsT0FBTyxHQUFHLE9BQU8sQ0FBQztRQUN2QixlQUFlLENBQUMsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsUUFBUSxDQUFDLENBQUMsQ0FBQyxTQUFTOzs7O1FBQUMsVUFBQSxJQUFJO1lBQzNELEtBQUksQ0FBQyxnQkFBZ0IsR0FBRyxJQUFJLENBQUMsY0FBYyxDQUFDLFFBQVEsS0FBSyxRQUFRLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxDQUFDLENBQUMsUUFBUSxDQUFDO1lBQ3JGLEtBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7UUFDMUIsQ0FBQyxFQUFDLENBQUM7SUFDTCxDQUFDOzs7O0lBRUQsMENBQUs7OztJQUFMO1FBQ0UsSUFBSSxDQUFDLElBQUksR0FBRyxLQUFLLENBQUM7UUFDbEIsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7O0lBRUQsbURBQWM7OztJQUFkO1FBQ0UsSUFBSSxDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUU7WUFDZCxJQUFJLENBQUMsT0FBTyxDQUFDLE9BQU8sRUFBRSxDQUFDO1NBQ3hCO0lBQ0gsQ0FBQztJQUlELHNEQUFzRDtJQUN0RCx1REFBdUQ7Ozs7OztJQUN2RCxnREFBVzs7Ozs7O0lBQVg7UUFDRSxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDM0IsQ0FBQzs7Z0JBakVGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUscUJBQXFCO29CQUMvQixVQUFVLEVBQUUsQ0FBQyxXQUFXLENBQUM7b0JBQ3pCLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLHlQQUFtRDtvQkFDbkQsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxTQUFTLEVBQUUsQ0FBQyxxQkFBcUIsQ0FBQzs2QkFFaEMseVBBYUM7aUJBRUo7Ozs7Z0JBcENDLGlCQUFpQjs7SUE4RW5CLGlDQUFDO0NBQUEsQUFsRUQsSUFrRUM7U0F6Q1ksMEJBQTBCOzs7SUFDckMsMENBQVk7O0lBQ1osaURBQStCOztJQUMvQixzREFBOEM7Ozs7O0lBQzlDLDZDQUFtQzs7Ozs7SUFDbkMsOENBQWlDOzs7OztJQTRCckIseUNBQThCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIE9uRGVzdHJveSxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgT2JzZXJ2YWJsZSwgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgc2xpZGVNb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi9zbGlkZSc7XG5pbXBvcnQgeyBOekRyb3Bkb3duU2VydmljZSB9IGZyb20gJy4vbnotZHJvcGRvd24uc2VydmljZSc7XG5pbXBvcnQgeyBOek1lbnVEcm9wZG93blNlcnZpY2UgfSBmcm9tICcuL256LW1lbnUtZHJvcGRvd24uc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWRyb3Bkb3duLWNvbnRleHQnLFxuICBhbmltYXRpb25zOiBbc2xpZGVNb3Rpb25dLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWRyb3Bkb3duLWNvbnRleHQuY29tcG9uZW50Lmh0bWwnLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgcHJvdmlkZXJzOiBbTnpNZW51RHJvcGRvd25TZXJ2aWNlXSxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgbnotZHJvcGRvd24tY29udGV4dCB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuXG4gICAgICAuYW50LWRyb3Bkb3duIHtcbiAgICAgICAgdG9wOiAxMDAlO1xuICAgICAgICBsZWZ0OiAwO1xuICAgICAgICBwb3NpdGlvbjogcmVsYXRpdmU7XG4gICAgICAgIHdpZHRoOiAxMDAlO1xuICAgICAgICBtYXJnaW4tdG9wOiA0cHg7XG4gICAgICAgIG1hcmdpbi1ib3R0b206IDRweDtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpEcm9wZG93bkNvbnRleHRDb21wb25lbnQgaW1wbGVtZW50cyBPbkRlc3Ryb3kge1xuICBvcGVuID0gdHJ1ZTtcbiAgdGVtcGxhdGVSZWY6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBkcm9wRG93blBvc2l0aW9uOiAndG9wJyB8ICdib3R0b20nID0gJ2JvdHRvbSc7XG4gIHByaXZhdGUgY29udHJvbDogTnpEcm9wZG93blNlcnZpY2U7XG4gIHByaXZhdGUgZGVzdHJveSQgPSBuZXcgU3ViamVjdCgpO1xuXG4gIGluaXQoXG4gICAgb3BlbjogYm9vbGVhbixcbiAgICB0ZW1wbGF0ZVJlZjogVGVtcGxhdGVSZWY8dm9pZD4sXG4gICAgcG9zaXRpb25DaGFuZ2VzOiBPYnNlcnZhYmxlPENvbm5lY3RlZE92ZXJsYXlQb3NpdGlvbkNoYW5nZT4sXG4gICAgY29udHJvbDogTnpEcm9wZG93blNlcnZpY2VcbiAgKTogdm9pZCB7XG4gICAgdGhpcy5vcGVuID0gb3BlbjtcbiAgICB0aGlzLnRlbXBsYXRlUmVmID0gdGVtcGxhdGVSZWY7XG4gICAgdGhpcy5jb250cm9sID0gY29udHJvbDtcbiAgICBwb3NpdGlvbkNoYW5nZXMucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95JCkpLnN1YnNjcmliZShkYXRhID0+IHtcbiAgICAgIHRoaXMuZHJvcERvd25Qb3NpdGlvbiA9IGRhdGEuY29ubmVjdGlvblBhaXIub3ZlcmxheVkgPT09ICdib3R0b20nID8gJ3RvcCcgOiAnYm90dG9tJztcbiAgICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICAgIH0pO1xuICB9XG5cbiAgY2xvc2UoKTogdm9pZCB7XG4gICAgdGhpcy5vcGVuID0gZmFsc2U7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cblxuICBhZnRlckFuaW1hdGlvbigpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMub3Blbikge1xuICAgICAgdGhpcy5jb250cm9sLmRpc3Bvc2UoKTtcbiAgICB9XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgLy8gVE9ETyBhdXRvIHNldCBkcm9wZG93biBjbGFzcyBhZnRlciB0aGUgYnVnIHJlc29sdmVkXG4gIC8qKiBodHRwczovL2dpdGh1Yi5jb20vYW5ndWxhci9hbmd1bGFyL2lzc3Vlcy8xNDg0MiAqKi9cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=