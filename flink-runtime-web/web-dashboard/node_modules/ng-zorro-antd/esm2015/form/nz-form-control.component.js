/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChild, ElementRef, Host, Input, Optional, Renderer2, ViewEncapsulation } from '@angular/core';
import { FormControl, FormControlName, NgControl } from '@angular/forms';
import { startWith } from 'rxjs/operators';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { toBoolean } from '../core/util/convert';
import { NzColDirective } from '../grid/nz-col.directive';
import { NzRowDirective } from '../grid/nz-row.directive';
import { NzFormItemComponent } from './nz-form-item.component';
export class NzFormControlComponent extends NzColDirective {
    /**
     * @param {?} nzUpdateHostClassService
     * @param {?} elementRef
     * @param {?} nzFormItemComponent
     * @param {?} nzRowDirective
     * @param {?} cdr
     * @param {?} renderer
     */
    constructor(nzUpdateHostClassService, elementRef, nzFormItemComponent, nzRowDirective, cdr, renderer) {
        super(nzUpdateHostClassService, elementRef, nzFormItemComponent || nzRowDirective, renderer);
        this.cdr = cdr;
        this._hasFeedback = false;
        this.controlClassMap = {};
        renderer.addClass(elementRef.nativeElement, 'ant-form-item-control-wrapper');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzHasFeedback(value) {
        this._hasFeedback = toBoolean(value);
        this.setControlClassMap();
    }
    /**
     * @return {?}
     */
    get nzHasFeedback() {
        return this._hasFeedback;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzValidateStatus(value) {
        if (value instanceof FormControl) {
            this.validateControl = value;
            this.validateString = null;
            this.watchControl();
        }
        else if (value instanceof FormControlName) {
            this.validateControl = value.control;
            this.validateString = null;
            this.watchControl();
        }
        else {
            this.validateString = value;
            this.validateControl = null;
            this.setControlClassMap();
        }
    }
    /**
     * @return {?}
     */
    removeSubscribe() {
        if (this.validateChanges) {
            this.validateChanges.unsubscribe();
            this.validateChanges = null;
        }
    }
    /**
     * @return {?}
     */
    watchControl() {
        this.removeSubscribe();
        /** miss detect https://github.com/angular/angular/issues/10887 **/
        if (this.validateControl && this.validateControl.statusChanges) {
            this.validateChanges = this.validateControl.statusChanges.pipe(startWith(null)).subscribe((/**
             * @return {?}
             */
            () => {
                this.setControlClassMap();
                this.cdr.markForCheck();
            }));
        }
    }
    /**
     * @param {?} status
     * @return {?}
     */
    validateControlStatus(status) {
        return (!!this.validateControl &&
            (this.validateControl.dirty || this.validateControl.touched) &&
            this.validateControl.status === status);
    }
    /**
     * @return {?}
     */
    setControlClassMap() {
        this.controlClassMap = {
            [`has-warning`]: this.validateString === 'warning',
            [`is-validating`]: this.validateString === 'validating' ||
                this.validateString === 'pending' ||
                this.validateControlStatus('PENDING'),
            [`has-error`]: this.validateString === 'error' || this.validateControlStatus('INVALID'),
            [`has-success`]: this.validateString === 'success' || this.validateControlStatus('VALID'),
            [`has-feedback`]: this.nzHasFeedback
        };
        if (this.controlClassMap['has-warning']) {
            this.iconType = 'exclamation-circle-fill';
        }
        else if (this.controlClassMap['is-validating']) {
            this.iconType = 'loading';
        }
        else if (this.controlClassMap['has-error']) {
            this.iconType = 'close-circle-fill';
        }
        else if (this.controlClassMap['has-success']) {
            this.iconType = 'check-circle-fill';
        }
        else {
            this.iconType = '';
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        super.ngOnInit();
        this.setControlClassMap();
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.removeSubscribe();
        super.ngOnDestroy();
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        if (this.defaultValidateControl && !this.validateControl && !this.validateString) {
            this.nzValidateStatus = this.defaultValidateControl;
        }
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        super.ngAfterViewInit();
    }
}
NzFormControlComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-form-control',
                preserveWhitespaces: false,
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                providers: [NzUpdateHostClassService],
                template: "<div class=\"ant-form-item-control\" [ngClass]=\"controlClassMap\">\n  <span class=\"ant-form-item-children\">\n    <ng-content></ng-content>\n    <span class=\"ant-form-item-children-icon\">\n      <i *ngIf=\"nzHasFeedback && iconType\" nz-icon [type]=\"iconType\"></i>\n    </span>\n  </span>\n  <ng-content select=\"nz-form-explain\"></ng-content>\n</div>",
                styles: [`
      nz-form-control {
        display: block;
      }
    `]
            }] }
];
/** @nocollapse */
NzFormControlComponent.ctorParameters = () => [
    { type: NzUpdateHostClassService },
    { type: ElementRef },
    { type: NzFormItemComponent, decorators: [{ type: Optional }, { type: Host }] },
    { type: NzRowDirective, decorators: [{ type: Optional }, { type: Host }] },
    { type: ChangeDetectorRef },
    { type: Renderer2 }
];
NzFormControlComponent.propDecorators = {
    defaultValidateControl: [{ type: ContentChild, args: [NgControl,] }],
    nzHasFeedback: [{ type: Input }],
    nzValidateStatus: [{ type: Input }]
};
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzFormControlComponent.prototype._hasFeedback;
    /** @type {?} */
    NzFormControlComponent.prototype.validateChanges;
    /** @type {?} */
    NzFormControlComponent.prototype.validateString;
    /** @type {?} */
    NzFormControlComponent.prototype.controlClassMap;
    /** @type {?} */
    NzFormControlComponent.prototype.iconType;
    /** @type {?} */
    NzFormControlComponent.prototype.validateControl;
    /** @type {?} */
    NzFormControlComponent.prototype.defaultValidateControl;
    /**
     * @type {?}
     * @private
     */
    NzFormControlComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZm9ybS1jb250cm9sLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJmb3JtL256LWZvcm0tY29udHJvbC5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFHTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxZQUFZLEVBQ1osVUFBVSxFQUNWLElBQUksRUFDSixLQUFLLEVBR0wsUUFBUSxFQUNSLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLFdBQVcsRUFBRSxlQUFlLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFekUsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzNDLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBRXRGLE9BQU8sRUFBRSxTQUFTLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNqRCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDMUQsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQzFELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBaUIvRCxNQUFNLE9BQU8sc0JBQXVCLFNBQVEsY0FBYzs7Ozs7Ozs7O0lBd0Z4RCxZQUNFLHdCQUFrRCxFQUNsRCxVQUFzQixFQUNGLG1CQUF3QyxFQUN4QyxjQUE4QixFQUMxQyxHQUFzQixFQUM5QixRQUFtQjtRQUVuQixLQUFLLENBQUMsd0JBQXdCLEVBQUUsVUFBVSxFQUFFLG1CQUFtQixJQUFJLGNBQWMsRUFBRSxRQUFRLENBQUMsQ0FBQztRQUhyRixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQTNGeEIsaUJBQVksR0FBRyxLQUFLLENBQUM7UUFHN0Isb0JBQWUsR0FBZ0IsRUFBRSxDQUFDO1FBNEZoQyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsK0JBQStCLENBQUMsQ0FBQztJQUMvRSxDQUFDOzs7OztJQXhGRCxJQUNJLGFBQWEsQ0FBQyxLQUFjO1FBQzlCLElBQUksQ0FBQyxZQUFZLEdBQUcsU0FBUyxDQUFDLEtBQUssQ0FBQyxDQUFDO1FBQ3JDLElBQUksQ0FBQyxrQkFBa0IsRUFBRSxDQUFDO0lBQzVCLENBQUM7Ozs7SUFFRCxJQUFJLGFBQWE7UUFDZixPQUFPLElBQUksQ0FBQyxZQUFZLENBQUM7SUFDM0IsQ0FBQzs7Ozs7SUFFRCxJQUNJLGdCQUFnQixDQUFDLEtBQTZDO1FBQ2hFLElBQUksS0FBSyxZQUFZLFdBQVcsRUFBRTtZQUNoQyxJQUFJLENBQUMsZUFBZSxHQUFHLEtBQUssQ0FBQztZQUM3QixJQUFJLENBQUMsY0FBYyxHQUFHLElBQUksQ0FBQztZQUMzQixJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7U0FDckI7YUFBTSxJQUFJLEtBQUssWUFBWSxlQUFlLEVBQUU7WUFDM0MsSUFBSSxDQUFDLGVBQWUsR0FBRyxLQUFLLENBQUMsT0FBTyxDQUFDO1lBQ3JDLElBQUksQ0FBQyxjQUFjLEdBQUcsSUFBSSxDQUFDO1lBQzNCLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztTQUNyQjthQUFNO1lBQ0wsSUFBSSxDQUFDLGNBQWMsR0FBRyxLQUFLLENBQUM7WUFDNUIsSUFBSSxDQUFDLGVBQWUsR0FBRyxJQUFJLENBQUM7WUFDNUIsSUFBSSxDQUFDLGtCQUFrQixFQUFFLENBQUM7U0FDM0I7SUFDSCxDQUFDOzs7O0lBRUQsZUFBZTtRQUNiLElBQUksSUFBSSxDQUFDLGVBQWUsRUFBRTtZQUN4QixJQUFJLENBQUMsZUFBZSxDQUFDLFdBQVcsRUFBRSxDQUFDO1lBQ25DLElBQUksQ0FBQyxlQUFlLEdBQUcsSUFBSSxDQUFDO1NBQzdCO0lBQ0gsQ0FBQzs7OztJQUVELFlBQVk7UUFDVixJQUFJLENBQUMsZUFBZSxFQUFFLENBQUM7UUFDdkIsbUVBQW1FO1FBQ25FLElBQUksSUFBSSxDQUFDLGVBQWUsSUFBSSxJQUFJLENBQUMsZUFBZSxDQUFDLGFBQWEsRUFBRTtZQUM5RCxJQUFJLENBQUMsZUFBZSxHQUFHLElBQUksQ0FBQyxlQUFlLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxTQUFTLENBQUMsSUFBSSxDQUFDLENBQUMsQ0FBQyxTQUFTOzs7WUFBQyxHQUFHLEVBQUU7Z0JBQzdGLElBQUksQ0FBQyxrQkFBa0IsRUFBRSxDQUFDO2dCQUMxQixJQUFJLENBQUMsR0FBRyxDQUFDLFlBQVksRUFBRSxDQUFDO1lBQzFCLENBQUMsRUFBQyxDQUFDO1NBQ0o7SUFDSCxDQUFDOzs7OztJQUVELHFCQUFxQixDQUFDLE1BQWM7UUFDbEMsT0FBTyxDQUNMLENBQUMsQ0FBQyxJQUFJLENBQUMsZUFBZTtZQUN0QixDQUFDLElBQUksQ0FBQyxlQUFlLENBQUMsS0FBSyxJQUFJLElBQUksQ0FBQyxlQUFlLENBQUMsT0FBTyxDQUFDO1lBQzVELElBQUksQ0FBQyxlQUFlLENBQUMsTUFBTSxLQUFLLE1BQU0sQ0FDdkMsQ0FBQztJQUNKLENBQUM7Ozs7SUFFRCxrQkFBa0I7UUFDaEIsSUFBSSxDQUFDLGVBQWUsR0FBRztZQUNyQixDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUksQ0FBQyxjQUFjLEtBQUssU0FBUztZQUNsRCxDQUFDLGVBQWUsQ0FBQyxFQUNmLElBQUksQ0FBQyxjQUFjLEtBQUssWUFBWTtnQkFDcEMsSUFBSSxDQUFDLGNBQWMsS0FBSyxTQUFTO2dCQUNqQyxJQUFJLENBQUMscUJBQXFCLENBQUMsU0FBUyxDQUFDO1lBQ3ZDLENBQUMsV0FBVyxDQUFDLEVBQUUsSUFBSSxDQUFDLGNBQWMsS0FBSyxPQUFPLElBQUksSUFBSSxDQUFDLHFCQUFxQixDQUFDLFNBQVMsQ0FBQztZQUN2RixDQUFDLGFBQWEsQ0FBQyxFQUFFLElBQUksQ0FBQyxjQUFjLEtBQUssU0FBUyxJQUFJLElBQUksQ0FBQyxxQkFBcUIsQ0FBQyxPQUFPLENBQUM7WUFDekYsQ0FBQyxjQUFjLENBQUMsRUFBRSxJQUFJLENBQUMsYUFBYTtTQUNyQyxDQUFDO1FBRUYsSUFBSSxJQUFJLENBQUMsZUFBZSxDQUFDLGFBQWEsQ0FBQyxFQUFFO1lBQ3ZDLElBQUksQ0FBQyxRQUFRLEdBQUcseUJBQXlCLENBQUM7U0FDM0M7YUFBTSxJQUFJLElBQUksQ0FBQyxlQUFlLENBQUMsZUFBZSxDQUFDLEVBQUU7WUFDaEQsSUFBSSxDQUFDLFFBQVEsR0FBRyxTQUFTLENBQUM7U0FDM0I7YUFBTSxJQUFJLElBQUksQ0FBQyxlQUFlLENBQUMsV0FBVyxDQUFDLEVBQUU7WUFDNUMsSUFBSSxDQUFDLFFBQVEsR0FBRyxtQkFBbUIsQ0FBQztTQUNyQzthQUFNLElBQUksSUFBSSxDQUFDLGVBQWUsQ0FBQyxhQUFhLENBQUMsRUFBRTtZQUM5QyxJQUFJLENBQUMsUUFBUSxHQUFHLG1CQUFtQixDQUFDO1NBQ3JDO2FBQU07WUFDTCxJQUFJLENBQUMsUUFBUSxHQUFHLEVBQUUsQ0FBQztTQUNwQjtJQUNILENBQUM7Ozs7SUFjRCxRQUFRO1FBQ04sS0FBSyxDQUFDLFFBQVEsRUFBRSxDQUFDO1FBQ2pCLElBQUksQ0FBQyxrQkFBa0IsRUFBRSxDQUFDO0lBQzVCLENBQUM7Ozs7SUFFRCxXQUFXO1FBQ1QsSUFBSSxDQUFDLGVBQWUsRUFBRSxDQUFDO1FBQ3ZCLEtBQUssQ0FBQyxXQUFXLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7O0lBRUQsa0JBQWtCO1FBQ2hCLElBQUksSUFBSSxDQUFDLHNCQUFzQixJQUFJLENBQUMsSUFBSSxDQUFDLGVBQWUsSUFBSSxDQUFDLElBQUksQ0FBQyxjQUFjLEVBQUU7WUFDaEYsSUFBSSxDQUFDLGdCQUFnQixHQUFHLElBQUksQ0FBQyxzQkFBc0IsQ0FBQztTQUNyRDtJQUNILENBQUM7Ozs7SUFFRCxlQUFlO1FBQ2IsS0FBSyxDQUFDLGVBQWUsRUFBRSxDQUFDO0lBQzFCLENBQUM7OztZQXJJRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLGlCQUFpQjtnQkFDM0IsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7Z0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxTQUFTLEVBQUUsQ0FBQyx3QkFBd0IsQ0FBQztnQkFDckMsa1hBQStDO3lCQUU3Qzs7OztLQUlDO2FBRUo7Ozs7WUFyQlEsd0JBQXdCO1lBWi9CLFVBQVU7WUFpQkgsbUJBQW1CLHVCQTRHdkIsUUFBUSxZQUFJLElBQUk7WUE3R1osY0FBYyx1QkE4R2xCLFFBQVEsWUFBSSxJQUFJO1lBakluQixpQkFBaUI7WUFTakIsU0FBUzs7O3FDQW9DUixZQUFZLFNBQUMsU0FBUzs0QkFFdEIsS0FBSzsrQkFVTCxLQUFLOzs7Ozs7O0lBbEJOLDhDQUE2Qjs7SUFDN0IsaURBQXFDOztJQUNyQyxnREFBOEI7O0lBQzlCLGlEQUFrQzs7SUFDbEMsMENBQWlCOztJQUNqQixpREFBb0M7O0lBQ3BDLHdEQUFpRTs7Ozs7SUFxRi9ELHFDQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIEFmdGVyQ29udGVudEluaXQsXG4gIEFmdGVyVmlld0luaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3B0aW9uYWwsXG4gIFJlbmRlcmVyMixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBGb3JtQ29udHJvbCwgRm9ybUNvbnRyb2xOYW1lLCBOZ0NvbnRyb2wgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5pbXBvcnQgeyBTdWJzY3JpcHRpb24gfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IHN0YXJ0V2l0aCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcbmltcG9ydCB7IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSB9IGZyb20gJy4uL2NvcmUvc2VydmljZXMvdXBkYXRlLWhvc3QtY2xhc3Muc2VydmljZSc7XG5pbXBvcnQgeyBOZ0NsYXNzVHlwZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvbmctY2xhc3MnO1xuaW1wb3J0IHsgdG9Cb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpDb2xEaXJlY3RpdmUgfSBmcm9tICcuLi9ncmlkL256LWNvbC5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpSb3dEaXJlY3RpdmUgfSBmcm9tICcuLi9ncmlkL256LXJvdy5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpGb3JtSXRlbUNvbXBvbmVudCB9IGZyb20gJy4vbnotZm9ybS1pdGVtLmNvbXBvbmVudCc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWZvcm0tY29udHJvbCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWZvcm0tY29udHJvbC5jb21wb25lbnQuaHRtbCcsXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIG56LWZvcm0tY29udHJvbCB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOekZvcm1Db250cm9sQ29tcG9uZW50IGV4dGVuZHMgTnpDb2xEaXJlY3RpdmVcbiAgaW1wbGVtZW50cyBPbkRlc3Ryb3ksIE9uSW5pdCwgQWZ0ZXJDb250ZW50SW5pdCwgQWZ0ZXJWaWV3SW5pdCwgT25EZXN0cm95IHtcbiAgcHJpdmF0ZSBfaGFzRmVlZGJhY2sgPSBmYWxzZTtcbiAgdmFsaWRhdGVDaGFuZ2VzOiBTdWJzY3JpcHRpb24gfCBudWxsO1xuICB2YWxpZGF0ZVN0cmluZzogc3RyaW5nIHwgbnVsbDtcbiAgY29udHJvbENsYXNzTWFwOiBOZ0NsYXNzVHlwZSA9IHt9O1xuICBpY29uVHlwZTogc3RyaW5nO1xuICB2YWxpZGF0ZUNvbnRyb2w6IEZvcm1Db250cm9sIHwgbnVsbDtcbiAgQENvbnRlbnRDaGlsZChOZ0NvbnRyb2wpIGRlZmF1bHRWYWxpZGF0ZUNvbnRyb2w6IEZvcm1Db250cm9sTmFtZTtcblxuICBASW5wdXQoKVxuICBzZXQgbnpIYXNGZWVkYmFjayh2YWx1ZTogYm9vbGVhbikge1xuICAgIHRoaXMuX2hhc0ZlZWRiYWNrID0gdG9Cb29sZWFuKHZhbHVlKTtcbiAgICB0aGlzLnNldENvbnRyb2xDbGFzc01hcCgpO1xuICB9XG5cbiAgZ2V0IG56SGFzRmVlZGJhY2soKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMuX2hhc0ZlZWRiYWNrO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56VmFsaWRhdGVTdGF0dXModmFsdWU6IHN0cmluZyB8IEZvcm1Db250cm9sIHwgRm9ybUNvbnRyb2xOYW1lKSB7XG4gICAgaWYgKHZhbHVlIGluc3RhbmNlb2YgRm9ybUNvbnRyb2wpIHtcbiAgICAgIHRoaXMudmFsaWRhdGVDb250cm9sID0gdmFsdWU7XG4gICAgICB0aGlzLnZhbGlkYXRlU3RyaW5nID0gbnVsbDtcbiAgICAgIHRoaXMud2F0Y2hDb250cm9sKCk7XG4gICAgfSBlbHNlIGlmICh2YWx1ZSBpbnN0YW5jZW9mIEZvcm1Db250cm9sTmFtZSkge1xuICAgICAgdGhpcy52YWxpZGF0ZUNvbnRyb2wgPSB2YWx1ZS5jb250cm9sO1xuICAgICAgdGhpcy52YWxpZGF0ZVN0cmluZyA9IG51bGw7XG4gICAgICB0aGlzLndhdGNoQ29udHJvbCgpO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnZhbGlkYXRlU3RyaW5nID0gdmFsdWU7XG4gICAgICB0aGlzLnZhbGlkYXRlQ29udHJvbCA9IG51bGw7XG4gICAgICB0aGlzLnNldENvbnRyb2xDbGFzc01hcCgpO1xuICAgIH1cbiAgfVxuXG4gIHJlbW92ZVN1YnNjcmliZSgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy52YWxpZGF0ZUNoYW5nZXMpIHtcbiAgICAgIHRoaXMudmFsaWRhdGVDaGFuZ2VzLnVuc3Vic2NyaWJlKCk7XG4gICAgICB0aGlzLnZhbGlkYXRlQ2hhbmdlcyA9IG51bGw7XG4gICAgfVxuICB9XG5cbiAgd2F0Y2hDb250cm9sKCk6IHZvaWQge1xuICAgIHRoaXMucmVtb3ZlU3Vic2NyaWJlKCk7XG4gICAgLyoqIG1pc3MgZGV0ZWN0IGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvaXNzdWVzLzEwODg3ICoqL1xuICAgIGlmICh0aGlzLnZhbGlkYXRlQ29udHJvbCAmJiB0aGlzLnZhbGlkYXRlQ29udHJvbC5zdGF0dXNDaGFuZ2VzKSB7XG4gICAgICB0aGlzLnZhbGlkYXRlQ2hhbmdlcyA9IHRoaXMudmFsaWRhdGVDb250cm9sLnN0YXR1c0NoYW5nZXMucGlwZShzdGFydFdpdGgobnVsbCkpLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICAgIHRoaXMuc2V0Q29udHJvbENsYXNzTWFwKCk7XG4gICAgICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgdmFsaWRhdGVDb250cm9sU3RhdHVzKHN0YXR1czogc3RyaW5nKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIChcbiAgICAgICEhdGhpcy52YWxpZGF0ZUNvbnRyb2wgJiZcbiAgICAgICh0aGlzLnZhbGlkYXRlQ29udHJvbC5kaXJ0eSB8fCB0aGlzLnZhbGlkYXRlQ29udHJvbC50b3VjaGVkKSAmJlxuICAgICAgdGhpcy52YWxpZGF0ZUNvbnRyb2wuc3RhdHVzID09PSBzdGF0dXNcbiAgICApO1xuICB9XG5cbiAgc2V0Q29udHJvbENsYXNzTWFwKCk6IHZvaWQge1xuICAgIHRoaXMuY29udHJvbENsYXNzTWFwID0ge1xuICAgICAgW2BoYXMtd2FybmluZ2BdOiB0aGlzLnZhbGlkYXRlU3RyaW5nID09PSAnd2FybmluZycsXG4gICAgICBbYGlzLXZhbGlkYXRpbmdgXTpcbiAgICAgICAgdGhpcy52YWxpZGF0ZVN0cmluZyA9PT0gJ3ZhbGlkYXRpbmcnIHx8XG4gICAgICAgIHRoaXMudmFsaWRhdGVTdHJpbmcgPT09ICdwZW5kaW5nJyB8fFxuICAgICAgICB0aGlzLnZhbGlkYXRlQ29udHJvbFN0YXR1cygnUEVORElORycpLFxuICAgICAgW2BoYXMtZXJyb3JgXTogdGhpcy52YWxpZGF0ZVN0cmluZyA9PT0gJ2Vycm9yJyB8fCB0aGlzLnZhbGlkYXRlQ29udHJvbFN0YXR1cygnSU5WQUxJRCcpLFxuICAgICAgW2BoYXMtc3VjY2Vzc2BdOiB0aGlzLnZhbGlkYXRlU3RyaW5nID09PSAnc3VjY2VzcycgfHwgdGhpcy52YWxpZGF0ZUNvbnRyb2xTdGF0dXMoJ1ZBTElEJyksXG4gICAgICBbYGhhcy1mZWVkYmFja2BdOiB0aGlzLm56SGFzRmVlZGJhY2tcbiAgICB9O1xuXG4gICAgaWYgKHRoaXMuY29udHJvbENsYXNzTWFwWydoYXMtd2FybmluZyddKSB7XG4gICAgICB0aGlzLmljb25UeXBlID0gJ2V4Y2xhbWF0aW9uLWNpcmNsZS1maWxsJztcbiAgICB9IGVsc2UgaWYgKHRoaXMuY29udHJvbENsYXNzTWFwWydpcy12YWxpZGF0aW5nJ10pIHtcbiAgICAgIHRoaXMuaWNvblR5cGUgPSAnbG9hZGluZyc7XG4gICAgfSBlbHNlIGlmICh0aGlzLmNvbnRyb2xDbGFzc01hcFsnaGFzLWVycm9yJ10pIHtcbiAgICAgIHRoaXMuaWNvblR5cGUgPSAnY2xvc2UtY2lyY2xlLWZpbGwnO1xuICAgIH0gZWxzZSBpZiAodGhpcy5jb250cm9sQ2xhc3NNYXBbJ2hhcy1zdWNjZXNzJ10pIHtcbiAgICAgIHRoaXMuaWNvblR5cGUgPSAnY2hlY2stY2lyY2xlLWZpbGwnO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLmljb25UeXBlID0gJyc7XG4gICAgfVxuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgbnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsXG4gICAgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBAT3B0aW9uYWwoKSBASG9zdCgpIG56Rm9ybUl0ZW1Db21wb25lbnQ6IE56Rm9ybUl0ZW1Db21wb25lbnQsXG4gICAgQE9wdGlvbmFsKCkgQEhvc3QoKSBuelJvd0RpcmVjdGl2ZTogTnpSb3dEaXJlY3RpdmUsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHJlbmRlcmVyOiBSZW5kZXJlcjJcbiAgKSB7XG4gICAgc3VwZXIobnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLCBlbGVtZW50UmVmLCBuekZvcm1JdGVtQ29tcG9uZW50IHx8IG56Um93RGlyZWN0aXZlLCByZW5kZXJlcik7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LWZvcm0taXRlbS1jb250cm9sLXdyYXBwZXInKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHN1cGVyLm5nT25Jbml0KCk7XG4gICAgdGhpcy5zZXRDb250cm9sQ2xhc3NNYXAoKTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMucmVtb3ZlU3Vic2NyaWJlKCk7XG4gICAgc3VwZXIubmdPbkRlc3Ryb3koKTtcbiAgfVxuXG4gIG5nQWZ0ZXJDb250ZW50SW5pdCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5kZWZhdWx0VmFsaWRhdGVDb250cm9sICYmICF0aGlzLnZhbGlkYXRlQ29udHJvbCAmJiAhdGhpcy52YWxpZGF0ZVN0cmluZykge1xuICAgICAgdGhpcy5uelZhbGlkYXRlU3RhdHVzID0gdGhpcy5kZWZhdWx0VmFsaWRhdGVDb250cm9sO1xuICAgIH1cbiAgfVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICBzdXBlci5uZ0FmdGVyVmlld0luaXQoKTtcbiAgfVxufVxuIl19