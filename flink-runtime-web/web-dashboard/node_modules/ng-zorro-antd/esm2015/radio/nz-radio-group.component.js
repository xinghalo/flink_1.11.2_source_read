/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, Input, QueryList, Renderer2, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { merge, Subject } from 'rxjs';
import { startWith, takeUntil } from 'rxjs/operators';
import { isNotNil } from '../core/util/check';
import { InputBoolean } from '../core/util/convert';
import { NzRadioComponent } from './nz-radio.component';
export class NzRadioGroupComponent {
    /**
     * @param {?} cdr
     * @param {?} renderer
     * @param {?} elementRef
     */
    constructor(cdr, renderer, elementRef) {
        this.cdr = cdr;
        this.destroy$ = new Subject();
        this.onChange = (/**
         * @return {?}
         */
        () => null);
        this.onTouched = (/**
         * @return {?}
         */
        () => null);
        this.nzButtonStyle = 'outline';
        this.nzSize = 'default';
        renderer.addClass(elementRef.nativeElement, 'ant-radio-group');
    }
    /**
     * @return {?}
     */
    updateChildrenStatus() {
        if (this.radios) {
            Promise.resolve().then((/**
             * @return {?}
             */
            () => {
                this.radios.forEach((/**
                 * @param {?} radio
                 * @return {?}
                 */
                radio => {
                    radio.checked = radio.nzValue === this.value;
                    if (isNotNil(this.nzDisabled)) {
                        radio.nzDisabled = this.nzDisabled;
                    }
                    if (this.nzName) {
                        radio.name = this.nzName;
                    }
                    radio.markForCheck();
                }));
            }));
        }
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.radios.changes
            .pipe(startWith(null), takeUntil(this.destroy$))
            .subscribe((/**
         * @return {?}
         */
        () => {
            this.updateChildrenStatus();
            if (this.selectSubscription) {
                this.selectSubscription.unsubscribe();
            }
            this.selectSubscription = merge(...this.radios.map((/**
             * @param {?} radio
             * @return {?}
             */
            radio => radio.select$)))
                .pipe(takeUntil(this.destroy$))
                .subscribe((/**
             * @param {?} radio
             * @return {?}
             */
            radio => {
                if (this.value !== radio.nzValue) {
                    this.value = radio.nzValue;
                    this.updateChildrenStatus();
                    this.onChange(this.value);
                }
            }));
            if (this.touchedSubscription) {
                this.touchedSubscription.unsubscribe();
            }
            this.touchedSubscription = merge(...this.radios.map((/**
             * @param {?} radio
             * @return {?}
             */
            radio => radio.touched$)))
                .pipe(takeUntil(this.destroy$))
                .subscribe((/**
             * @return {?}
             */
            () => {
                Promise.resolve().then((/**
                 * @return {?}
                 */
                () => this.onTouched()));
            }));
        }));
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzDisabled || changes.nzName) {
            this.updateChildrenStatus();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
    /* tslint:disable-next-line:no-any */
    /**
     * @param {?} value
     * @return {?}
     */
    writeValue(value) {
        this.value = value;
        this.updateChildrenStatus();
        this.cdr.markForCheck();
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChange = fn;
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouched = fn;
    }
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    setDisabledState(isDisabled) {
        this.nzDisabled = isDisabled;
        this.cdr.markForCheck();
    }
}
NzRadioGroupComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-radio-group',
                preserveWhitespaces: false,
                template: "<ng-content></ng-content>",
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                providers: [
                    {
                        provide: NG_VALUE_ACCESSOR,
                        useExisting: forwardRef((/**
                         * @return {?}
                         */
                        () => NzRadioGroupComponent)),
                        multi: true
                    }
                ],
                host: {
                    '[class.ant-radio-group-large]': `nzSize === 'large'`,
                    '[class.ant-radio-group-small]': `nzSize === 'small'`,
                    '[class.ant-radio-group-solid]': `nzButtonStyle === 'solid'`
                }
            }] }
];
/** @nocollapse */
NzRadioGroupComponent.ctorParameters = () => [
    { type: ChangeDetectorRef },
    { type: Renderer2 },
    { type: ElementRef }
];
NzRadioGroupComponent.propDecorators = {
    radios: [{ type: ContentChildren, args: [forwardRef((/**
                 * @return {?}
                 */
                () => NzRadioComponent)), { descendants: true },] }],
    nzDisabled: [{ type: Input }],
    nzButtonStyle: [{ type: Input }],
    nzSize: [{ type: Input }],
    nzName: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Boolean)
], NzRadioGroupComponent.prototype, "nzDisabled", void 0);
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzRadioGroupComponent.prototype.value;
    /**
     * @type {?}
     * @private
     */
    NzRadioGroupComponent.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzRadioGroupComponent.prototype.selectSubscription;
    /**
     * @type {?}
     * @private
     */
    NzRadioGroupComponent.prototype.touchedSubscription;
    /** @type {?} */
    NzRadioGroupComponent.prototype.onChange;
    /** @type {?} */
    NzRadioGroupComponent.prototype.onTouched;
    /** @type {?} */
    NzRadioGroupComponent.prototype.radios;
    /** @type {?} */
    NzRadioGroupComponent.prototype.nzDisabled;
    /** @type {?} */
    NzRadioGroupComponent.prototype.nzButtonStyle;
    /** @type {?} */
    NzRadioGroupComponent.prototype.nzSize;
    /** @type {?} */
    NzRadioGroupComponent.prototype.nzName;
    /**
     * @type {?}
     * @private
     */
    NzRadioGroupComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcmFkaW8tZ3JvdXAuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInJhZGlvL256LXJhZGlvLWdyb3VwLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCxVQUFVLEVBRVYsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsZUFBZSxFQUNmLFVBQVUsRUFDVixLQUFLLEVBR0wsU0FBUyxFQUNULFNBQVMsRUFFVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUF3QixpQkFBaUIsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQ3pFLE9BQU8sRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFnQixNQUFNLE1BQU0sQ0FBQztBQUNwRCxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRXRELE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUM5QyxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUF1QnhELE1BQU0sT0FBTyxxQkFBcUI7Ozs7OztJQStCaEMsWUFBb0IsR0FBc0IsRUFBRSxRQUFtQixFQUFFLFVBQXNCO1FBQW5FLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBNUJsQyxhQUFRLEdBQUcsSUFBSSxPQUFPLEVBQUUsQ0FBQztRQUdqQyxhQUFROzs7UUFBd0IsR0FBRyxFQUFFLENBQUMsSUFBSSxFQUFDO1FBQzNDLGNBQVM7OztRQUFlLEdBQUcsRUFBRSxDQUFDLElBQUksRUFBQztRQUcxQixrQkFBYSxHQUF1QixTQUFTLENBQUM7UUFDOUMsV0FBTSxHQUFrQixTQUFTLENBQUM7UUFxQnpDLFFBQVEsQ0FBQyxRQUFRLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxDQUFDO0lBQ2pFLENBQUM7Ozs7SUFuQkQsb0JBQW9CO1FBQ2xCLElBQUksSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNmLE9BQU8sQ0FBQyxPQUFPLEVBQUUsQ0FBQyxJQUFJOzs7WUFBQyxHQUFHLEVBQUU7Z0JBQzFCLElBQUksQ0FBQyxNQUFNLENBQUMsT0FBTzs7OztnQkFBQyxLQUFLLENBQUMsRUFBRTtvQkFDMUIsS0FBSyxDQUFDLE9BQU8sR0FBRyxLQUFLLENBQUMsT0FBTyxLQUFLLElBQUksQ0FBQyxLQUFLLENBQUM7b0JBQzdDLElBQUksUUFBUSxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsRUFBRTt3QkFDN0IsS0FBSyxDQUFDLFVBQVUsR0FBRyxJQUFJLENBQUMsVUFBVSxDQUFDO3FCQUNwQztvQkFDRCxJQUFJLElBQUksQ0FBQyxNQUFNLEVBQUU7d0JBQ2YsS0FBSyxDQUFDLElBQUksR0FBRyxJQUFJLENBQUMsTUFBTSxDQUFDO3FCQUMxQjtvQkFDRCxLQUFLLENBQUMsWUFBWSxFQUFFLENBQUM7Z0JBQ3ZCLENBQUMsRUFBQyxDQUFDO1lBQ0wsQ0FBQyxFQUFDLENBQUM7U0FDSjtJQUNILENBQUM7Ozs7SUFNRCxrQkFBa0I7UUFDaEIsSUFBSSxDQUFDLE1BQU0sQ0FBQyxPQUFPO2FBQ2hCLElBQUksQ0FDSCxTQUFTLENBQUMsSUFBSSxDQUFDLEVBQ2YsU0FBUyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FDekI7YUFDQSxTQUFTOzs7UUFBQyxHQUFHLEVBQUU7WUFDZCxJQUFJLENBQUMsb0JBQW9CLEVBQUUsQ0FBQztZQUM1QixJQUFJLElBQUksQ0FBQyxrQkFBa0IsRUFBRTtnQkFDM0IsSUFBSSxDQUFDLGtCQUFrQixDQUFDLFdBQVcsRUFBRSxDQUFDO2FBQ3ZDO1lBQ0QsSUFBSSxDQUFDLGtCQUFrQixHQUFHLEtBQUssQ0FBQyxHQUFHLElBQUksQ0FBQyxNQUFNLENBQUMsR0FBRzs7OztZQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsS0FBSyxDQUFDLE9BQU8sRUFBQyxDQUFDO2lCQUN4RSxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztpQkFDOUIsU0FBUzs7OztZQUFDLEtBQUssQ0FBQyxFQUFFO2dCQUNqQixJQUFJLElBQUksQ0FBQyxLQUFLLEtBQUssS0FBSyxDQUFDLE9BQU8sRUFBRTtvQkFDaEMsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUMsT0FBTyxDQUFDO29CQUMzQixJQUFJLENBQUMsb0JBQW9CLEVBQUUsQ0FBQztvQkFDNUIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDLENBQUM7aUJBQzNCO1lBQ0gsQ0FBQyxFQUFDLENBQUM7WUFDTCxJQUFJLElBQUksQ0FBQyxtQkFBbUIsRUFBRTtnQkFDNUIsSUFBSSxDQUFDLG1CQUFtQixDQUFDLFdBQVcsRUFBRSxDQUFDO2FBQ3hDO1lBQ0QsSUFBSSxDQUFDLG1CQUFtQixHQUFHLEtBQUssQ0FBQyxHQUFHLElBQUksQ0FBQyxNQUFNLENBQUMsR0FBRzs7OztZQUFDLEtBQUssQ0FBQyxFQUFFLENBQUMsS0FBSyxDQUFDLFFBQVEsRUFBQyxDQUFDO2lCQUMxRSxJQUFJLENBQUMsU0FBUyxDQUFDLElBQUksQ0FBQyxRQUFRLENBQUMsQ0FBQztpQkFDOUIsU0FBUzs7O1lBQUMsR0FBRyxFQUFFO2dCQUNkLE9BQU8sQ0FBQyxPQUFPLEVBQUUsQ0FBQyxJQUFJOzs7Z0JBQUMsR0FBRyxFQUFFLENBQUMsSUFBSSxDQUFDLFNBQVMsRUFBRSxFQUFDLENBQUM7WUFDakQsQ0FBQyxFQUFDLENBQUM7UUFDUCxDQUFDLEVBQUMsQ0FBQztJQUNQLENBQUM7Ozs7O0lBRUQsV0FBVyxDQUFDLE9BQXNCO1FBQ2hDLElBQUksT0FBTyxDQUFDLFVBQVUsSUFBSSxPQUFPLENBQUMsTUFBTSxFQUFFO1lBQ3hDLElBQUksQ0FBQyxvQkFBb0IsRUFBRSxDQUFDO1NBQzdCO0lBQ0gsQ0FBQzs7OztJQUVELFdBQVc7UUFDVCxJQUFJLENBQUMsUUFBUSxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ3JCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxFQUFFLENBQUM7SUFDM0IsQ0FBQzs7Ozs7O0lBR0QsVUFBVSxDQUFDLEtBQVU7UUFDbkIsSUFBSSxDQUFDLEtBQUssR0FBRyxLQUFLLENBQUM7UUFDbkIsSUFBSSxDQUFDLG9CQUFvQixFQUFFLENBQUM7UUFDNUIsSUFBSSxDQUFDLEdBQUcsQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7OztJQUVELGdCQUFnQixDQUFDLEVBQXVCO1FBQ3RDLElBQUksQ0FBQyxRQUFRLEdBQUcsRUFBRSxDQUFDO0lBQ3JCLENBQUM7Ozs7O0lBRUQsaUJBQWlCLENBQUMsRUFBYztRQUM5QixJQUFJLENBQUMsU0FBUyxHQUFHLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7OztJQUVELGdCQUFnQixDQUFDLFVBQW1CO1FBQ2xDLElBQUksQ0FBQyxVQUFVLEdBQUcsVUFBVSxDQUFDO1FBQzdCLElBQUksQ0FBQyxHQUFHLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7O1lBbEhGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsZ0JBQWdCO2dCQUMxQixtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQixxQ0FBOEM7Z0JBQzlDLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsU0FBUyxFQUFFO29CQUNUO3dCQUNFLE9BQU8sRUFBRSxpQkFBaUI7d0JBQzFCLFdBQVcsRUFBRSxVQUFVOzs7d0JBQUMsR0FBRyxFQUFFLENBQUMscUJBQXFCLEVBQUM7d0JBQ3BELEtBQUssRUFBRSxJQUFJO3FCQUNaO2lCQUNGO2dCQUNELElBQUksRUFBRTtvQkFDSiwrQkFBK0IsRUFBRSxvQkFBb0I7b0JBQ3JELCtCQUErQixFQUFFLG9CQUFvQjtvQkFDckQsK0JBQStCLEVBQUUsMkJBQTJCO2lCQUM3RDthQUNGOzs7O1lBeENDLGlCQUFpQjtZQVFqQixTQUFTO1lBTFQsVUFBVTs7O3FCQThDVCxlQUFlLFNBQUMsVUFBVTs7O2dCQUFDLEdBQUcsRUFBRSxDQUFDLGdCQUFnQixFQUFDLEVBQUUsRUFBRSxXQUFXLEVBQUUsSUFBSSxFQUFFO3lCQUN6RSxLQUFLOzRCQUNMLEtBQUs7cUJBQ0wsS0FBSztxQkFDTCxLQUFLOztBQUhtQjtJQUFmLFlBQVksRUFBRTs7eURBQXFCOzs7Ozs7SUFQN0Msc0NBQW1COzs7OztJQUNuQix5Q0FBaUM7Ozs7O0lBQ2pDLG1EQUF5Qzs7Ozs7SUFDekMsb0RBQTBDOztJQUMxQyx5Q0FBMkM7O0lBQzNDLDBDQUFtQzs7SUFDbkMsdUNBQWdIOztJQUNoSCwyQ0FBNkM7O0lBQzdDLDhDQUF1RDs7SUFDdkQsdUNBQTJDOztJQUMzQyx1Q0FBd0I7Ozs7O0lBbUJaLG9DQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIGZvcndhcmRSZWYsXG4gIEFmdGVyQ29udGVudEluaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGRyZW4sXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uRGVzdHJveSxcbiAgUXVlcnlMaXN0LFxuICBSZW5kZXJlcjIsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQ29udHJvbFZhbHVlQWNjZXNzb3IsIE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgbWVyZ2UsIFN1YmplY3QsIFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgc3RhcnRXaXRoLCB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBOelNpemVMRFNUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9zaXplJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56UmFkaW9Db21wb25lbnQgfSBmcm9tICcuL256LXJhZGlvLmNvbXBvbmVudCc7XG5cbmV4cG9ydCB0eXBlIE56UmFkaW9CdXR0b25TdHlsZSA9ICdvdXRsaW5lJyB8ICdzb2xpZCc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LXJhZGlvLWdyb3VwJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1yYWRpby1ncm91cC5jb21wb25lbnQuaHRtbCcsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBwcm92aWRlcnM6IFtcbiAgICB7XG4gICAgICBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUixcbiAgICAgIHVzZUV4aXN0aW5nOiBmb3J3YXJkUmVmKCgpID0+IE56UmFkaW9Hcm91cENvbXBvbmVudCksXG4gICAgICBtdWx0aTogdHJ1ZVxuICAgIH1cbiAgXSxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LXJhZGlvLWdyb3VwLWxhcmdlXSc6IGBuelNpemUgPT09ICdsYXJnZSdgLFxuICAgICdbY2xhc3MuYW50LXJhZGlvLWdyb3VwLXNtYWxsXSc6IGBuelNpemUgPT09ICdzbWFsbCdgLFxuICAgICdbY2xhc3MuYW50LXJhZGlvLWdyb3VwLXNvbGlkXSc6IGBuekJ1dHRvblN0eWxlID09PSAnc29saWQnYFxuICB9XG59KVxuZXhwb3J0IGNsYXNzIE56UmFkaW9Hcm91cENvbXBvbmVudCBpbXBsZW1lbnRzIEFmdGVyQ29udGVudEluaXQsIENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBPbkRlc3Ryb3ksIE9uQ2hhbmdlcyB7XG4gIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbiAgcHJpdmF0ZSB2YWx1ZTogYW55O1xuICBwcml2YXRlIGRlc3Ryb3kkID0gbmV3IFN1YmplY3QoKTtcbiAgcHJpdmF0ZSBzZWxlY3RTdWJzY3JpcHRpb246IFN1YnNjcmlwdGlvbjtcbiAgcHJpdmF0ZSB0b3VjaGVkU3Vic2NyaXB0aW9uOiBTdWJzY3JpcHRpb247XG4gIG9uQ2hhbmdlOiAoXzogc3RyaW5nKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgb25Ub3VjaGVkOiAoKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgQENvbnRlbnRDaGlsZHJlbihmb3J3YXJkUmVmKCgpID0+IE56UmFkaW9Db21wb25lbnQpLCB7IGRlc2NlbmRhbnRzOiB0cnVlIH0pIHJhZGlvczogUXVlcnlMaXN0PE56UmFkaW9Db21wb25lbnQ+O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZDogYm9vbGVhbjtcbiAgQElucHV0KCkgbnpCdXR0b25TdHlsZTogTnpSYWRpb0J1dHRvblN0eWxlID0gJ291dGxpbmUnO1xuICBASW5wdXQoKSBuelNpemU6IE56U2l6ZUxEU1R5cGUgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56TmFtZTogc3RyaW5nO1xuXG4gIHVwZGF0ZUNoaWxkcmVuU3RhdHVzKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnJhZGlvcykge1xuICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICAgIHRoaXMucmFkaW9zLmZvckVhY2gocmFkaW8gPT4ge1xuICAgICAgICAgIHJhZGlvLmNoZWNrZWQgPSByYWRpby5uelZhbHVlID09PSB0aGlzLnZhbHVlO1xuICAgICAgICAgIGlmIChpc05vdE5pbCh0aGlzLm56RGlzYWJsZWQpKSB7XG4gICAgICAgICAgICByYWRpby5uekRpc2FibGVkID0gdGhpcy5uekRpc2FibGVkO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAodGhpcy5uek5hbWUpIHtcbiAgICAgICAgICAgIHJhZGlvLm5hbWUgPSB0aGlzLm56TmFtZTtcbiAgICAgICAgICB9XG4gICAgICAgICAgcmFkaW8ubWFya0ZvckNoZWNrKCk7XG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLCByZW5kZXJlcjogUmVuZGVyZXIyLCBlbGVtZW50UmVmOiBFbGVtZW50UmVmKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LXJhZGlvLWdyb3VwJyk7XG4gIH1cblxuICBuZ0FmdGVyQ29udGVudEluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5yYWRpb3MuY2hhbmdlc1xuICAgICAgLnBpcGUoXG4gICAgICAgIHN0YXJ0V2l0aChudWxsKSxcbiAgICAgICAgdGFrZVVudGlsKHRoaXMuZGVzdHJveSQpXG4gICAgICApXG4gICAgICAuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgdGhpcy51cGRhdGVDaGlsZHJlblN0YXR1cygpO1xuICAgICAgICBpZiAodGhpcy5zZWxlY3RTdWJzY3JpcHRpb24pIHtcbiAgICAgICAgICB0aGlzLnNlbGVjdFN1YnNjcmlwdGlvbi51bnN1YnNjcmliZSgpO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMuc2VsZWN0U3Vic2NyaXB0aW9uID0gbWVyZ2UoLi4udGhpcy5yYWRpb3MubWFwKHJhZGlvID0+IHJhZGlvLnNlbGVjdCQpKVxuICAgICAgICAgIC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSlcbiAgICAgICAgICAuc3Vic2NyaWJlKHJhZGlvID0+IHtcbiAgICAgICAgICAgIGlmICh0aGlzLnZhbHVlICE9PSByYWRpby5uelZhbHVlKSB7XG4gICAgICAgICAgICAgIHRoaXMudmFsdWUgPSByYWRpby5uelZhbHVlO1xuICAgICAgICAgICAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RhdHVzKCk7XG4gICAgICAgICAgICAgIHRoaXMub25DaGFuZ2UodGhpcy52YWx1ZSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfSk7XG4gICAgICAgIGlmICh0aGlzLnRvdWNoZWRTdWJzY3JpcHRpb24pIHtcbiAgICAgICAgICB0aGlzLnRvdWNoZWRTdWJzY3JpcHRpb24udW5zdWJzY3JpYmUoKTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLnRvdWNoZWRTdWJzY3JpcHRpb24gPSBtZXJnZSguLi50aGlzLnJhZGlvcy5tYXAocmFkaW8gPT4gcmFkaW8udG91Y2hlZCQpKVxuICAgICAgICAgIC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSlcbiAgICAgICAgICAuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgICAgIFByb21pc2UucmVzb2x2ZSgpLnRoZW4oKCkgPT4gdGhpcy5vblRvdWNoZWQoKSk7XG4gICAgICAgICAgfSk7XG4gICAgICB9KTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uekRpc2FibGVkIHx8IGNoYW5nZXMubnpOYW1lKSB7XG4gICAgICB0aGlzLnVwZGF0ZUNoaWxkcmVuU3RhdHVzKCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG5cbiAgLyogdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueSAqL1xuICB3cml0ZVZhbHVlKHZhbHVlOiBhbnkpOiB2b2lkIHtcbiAgICB0aGlzLnZhbHVlID0gdmFsdWU7XG4gICAgdGhpcy51cGRhdGVDaGlsZHJlblN0YXR1cygpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgcmVnaXN0ZXJPbkNoYW5nZShmbjogKF86IHN0cmluZykgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25DaGFuZ2UgPSBmbjtcbiAgfVxuXG4gIHJlZ2lzdGVyT25Ub3VjaGVkKGZuOiAoKSA9PiB2b2lkKTogdm9pZCB7XG4gICAgdGhpcy5vblRvdWNoZWQgPSBmbjtcbiAgfVxuXG4gIHNldERpc2FibGVkU3RhdGUoaXNEaXNhYmxlZDogYm9vbGVhbik6IHZvaWQge1xuICAgIHRoaXMubnpEaXNhYmxlZCA9IGlzRGlzYWJsZWQ7XG4gICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gIH1cbn1cbiJdfQ==