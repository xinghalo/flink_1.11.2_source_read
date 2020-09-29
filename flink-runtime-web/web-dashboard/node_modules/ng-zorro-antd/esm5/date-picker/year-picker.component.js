/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Host, Input, Optional, Renderer2, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { DateHelperService } from '../i18n/date-helper.service';
import { HeaderPickerComponent } from './header-picker.component';
var NzYearPickerComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzYearPickerComponent, _super);
    function NzYearPickerComponent(i18n, cdr, dateHelper, renderer, elementRef, noAnimation) {
        var _this = _super.call(this, i18n, cdr, dateHelper, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this.nzFormat = 'yyyy';
        _this.endPanelMode = 'year';
        renderer.addClass(elementRef.nativeElement, 'ant-calendar-picker');
        return _this;
    }
    NzYearPickerComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    selector: 'nz-year-picker',
                    template: "<nz-picker\n  [isRange]=\"false\"\n  [value]=\"nzValue\"\n  (valueChange)=\"onValueChange($event)\"\n  [open]=\"nzOpen\"\n  [disabled]=\"nzDisabled\"\n  [format]=\"nzFormat\"\n  [allowClear]=\"nzAllowClear\"\n  [autoFocus]=\"nzAutoFocus\"\n  [className]=\"nzClassName\"\n  [placeholder]=\"nzPlaceHolder\"\n  [size]=\"nzSize\"\n  [style]=\"nzStyle\"\n  [noAnimation]=\"noAnimation?.nzNoAnimation\"\n  (openChange)=\"onOpenChange($event)\"\n>\n  <div *ngIf=\"realOpenState\">\n    <div class=\"ant-calendar-picker-container {{ nzDropdownClassName }} ant-calendar-picker-container-placement-bottomLeft\" [ngStyle]=\"nzPopupStyle\">\n      <div class=\"ant-calendar ant-calendar-month ant-calendar-month-calendar\" tabindex=\"0\">\n        <div class=\"ant-calendar-month-calendar-content\">\n          <div class=\"ant-calendar-month-header-wrap\">\n            <calendar-header\n              [disabledMonth]=\"nzDisabledDate\"\n              [disabledYear]=\"nzDisabledDate\"\n              [panelMode]=\"panelMode\"\n              (panelModeChange)=\"onPanelModeChange($event)\"\n              [value]=\"nzValue\"\n              (chooseYear)=\"onChooseValue('year', $event)\"\n              (chooseMonth)=\"onChooseValue('month', $event)\"\n              [locale]=\"nzLocale.lang\"\n              [enablePrev]=\"true\"\n              [enableNext]=\"true\"\n            ></calendar-header>\n          </div>\n          <calendar-footer *ngIf=\"extraFooter\" [extraFooter]=\"extraFooter\"></calendar-footer>\n        </div>\n      </div>\n    </div>\n  </div>\n</nz-picker>",
                    providers: [
                        {
                            provide: NG_VALUE_ACCESSOR,
                            multi: true,
                            useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzYearPickerComponent; }))
                        }
                    ]
                }] }
    ];
    /** @nocollapse */
    NzYearPickerComponent.ctorParameters = function () { return [
        { type: NzI18nService },
        { type: ChangeDetectorRef },
        { type: DateHelperService },
        { type: Renderer2 },
        { type: ElementRef },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzYearPickerComponent.propDecorators = {
        nzFormat: [{ type: Input }]
    };
    return NzYearPickerComponent;
}(HeaderPickerComponent));
export { NzYearPickerComponent };
if (false) {
    /** @type {?} */
    NzYearPickerComponent.prototype.nzFormat;
    /** @type {?} */
    NzYearPickerComponent.prototype.endPanelMode;
    /** @type {?} */
    NzYearPickerComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoieWVhci1waWNrZXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImRhdGUtcGlja2VyL3llYXItcGlja2VyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCxVQUFVLEVBQ1YsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsVUFBVSxFQUNWLElBQUksRUFDSixLQUFLLEVBQ0wsUUFBUSxFQUNSLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFbkQsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBRXhELE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ2hFLE9BQU8sRUFBRSxxQkFBcUIsRUFBc0IsTUFBTSwyQkFBMkIsQ0FBQztBQUV0RjtJQWEyQyxpREFBcUI7SUFLOUQsK0JBQ0UsSUFBbUIsRUFDbkIsR0FBc0IsRUFDdEIsVUFBNkIsRUFDN0IsUUFBbUIsRUFDbkIsVUFBc0IsRUFDSyxXQUFvQztRQU5qRSxZQVFFLGtCQUFNLElBQUksRUFBRSxHQUFHLEVBQUUsVUFBVSxFQUFFLFdBQVcsQ0FBQyxTQUUxQztRQUo0QixpQkFBVyxHQUFYLFdBQVcsQ0FBeUI7UUFWeEQsY0FBUSxHQUFXLE1BQU0sQ0FBQztRQUVuQyxrQkFBWSxHQUF1QixNQUFNLENBQUM7UUFXeEMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLHFCQUFxQixDQUFDLENBQUM7O0lBQ3JFLENBQUM7O2dCQTVCRixTQUFTLFNBQUM7b0JBQ1QsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7b0JBQ3JDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxRQUFRLEVBQUUsZ0JBQWdCO29CQUMxQiwraURBQTZDO29CQUM3QyxTQUFTLEVBQUU7d0JBQ1Q7NEJBQ0UsT0FBTyxFQUFFLGlCQUFpQjs0QkFDMUIsS0FBSyxFQUFFLElBQUk7NEJBQ1gsV0FBVyxFQUFFLFVBQVU7Ozs0QkFBQyxjQUFNLE9BQUEscUJBQXFCLEVBQXJCLENBQXFCLEVBQUM7eUJBQ3JEO3FCQUNGO2lCQUNGOzs7O2dCQWpCUSxhQUFhO2dCQVpwQixpQkFBaUI7Z0JBY1YsaUJBQWlCO2dCQVJ4QixTQUFTO2dCQUpULFVBQVU7Z0JBU0gsc0JBQXNCLHVCQThCMUIsSUFBSSxZQUFJLFFBQVE7OzsyQkFWbEIsS0FBSzs7SUFlUiw0QkFBQztDQUFBLEFBN0JELENBYTJDLHFCQUFxQixHQWdCL0Q7U0FoQlkscUJBQXFCOzs7SUFDaEMseUNBQW1DOztJQUVuQyw2Q0FBMEM7O0lBUXhDLDRDQUErRCIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIGZvcndhcmRSZWYsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBIb3N0LFxuICBJbnB1dCxcbiAgT3B0aW9uYWwsXG4gIFJlbmRlcmVyMixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBOR19WQUxVRV9BQ0NFU1NPUiB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpJMThuU2VydmljZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5zZXJ2aWNlJztcblxuaW1wb3J0IHsgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgSGVhZGVyUGlja2VyQ29tcG9uZW50LCBTdXBwb3J0SGVhZGVyUGFuZWwgfSBmcm9tICcuL2hlYWRlci1waWNrZXIuY29tcG9uZW50JztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBzZWxlY3RvcjogJ256LXllYXItcGlja2VyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL2hlYWRlci1waWNrZXIuY29tcG9uZW50Lmh0bWwnLFxuICBwcm92aWRlcnM6IFtcbiAgICB7XG4gICAgICBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUixcbiAgICAgIG11bHRpOiB0cnVlLFxuICAgICAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpZZWFyUGlja2VyQ29tcG9uZW50KVxuICAgIH1cbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOelllYXJQaWNrZXJDb21wb25lbnQgZXh0ZW5kcyBIZWFkZXJQaWNrZXJDb21wb25lbnQge1xuICBASW5wdXQoKSBuekZvcm1hdDogc3RyaW5nID0gJ3l5eXknO1xuXG4gIGVuZFBhbmVsTW9kZTogU3VwcG9ydEhlYWRlclBhbmVsID0gJ3llYXInO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGkxOG46IE56STE4blNlcnZpY2UsXG4gICAgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSxcbiAgICByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlXG4gICkge1xuICAgIHN1cGVyKGkxOG4sIGNkciwgZGF0ZUhlbHBlciwgbm9BbmltYXRpb24pO1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1jYWxlbmRhci1waWNrZXInKTtcbiAgfVxufVxuIl19