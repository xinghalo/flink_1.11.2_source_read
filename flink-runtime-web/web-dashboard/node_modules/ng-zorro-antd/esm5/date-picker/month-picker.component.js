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
var NzMonthPickerComponent = /** @class */ (function (_super) {
    tslib_1.__extends(NzMonthPickerComponent, _super);
    function NzMonthPickerComponent(i18n, cdr, dateHelper, renderer, elementRef, noAnimation) {
        var _this = _super.call(this, i18n, cdr, dateHelper, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this.nzFormat = 'yyyy-MM';
        _this.endPanelMode = 'month';
        renderer.addClass(elementRef.nativeElement, 'ant-calendar-picker');
        return _this;
    }
    NzMonthPickerComponent.decorators = [
        { type: Component, args: [{
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    selector: 'nz-month-picker',
                    template: "<nz-picker\n  [isRange]=\"false\"\n  [value]=\"nzValue\"\n  (valueChange)=\"onValueChange($event)\"\n  [open]=\"nzOpen\"\n  [disabled]=\"nzDisabled\"\n  [format]=\"nzFormat\"\n  [allowClear]=\"nzAllowClear\"\n  [autoFocus]=\"nzAutoFocus\"\n  [className]=\"nzClassName\"\n  [placeholder]=\"nzPlaceHolder\"\n  [size]=\"nzSize\"\n  [style]=\"nzStyle\"\n  [noAnimation]=\"noAnimation?.nzNoAnimation\"\n  (openChange)=\"onOpenChange($event)\"\n>\n  <div *ngIf=\"realOpenState\">\n    <div class=\"ant-calendar-picker-container {{ nzDropdownClassName }} ant-calendar-picker-container-placement-bottomLeft\" [ngStyle]=\"nzPopupStyle\">\n      <div class=\"ant-calendar ant-calendar-month ant-calendar-month-calendar\" tabindex=\"0\">\n        <div class=\"ant-calendar-month-calendar-content\">\n          <div class=\"ant-calendar-month-header-wrap\">\n            <calendar-header\n              [disabledMonth]=\"nzDisabledDate\"\n              [disabledYear]=\"nzDisabledDate\"\n              [panelMode]=\"panelMode\"\n              (panelModeChange)=\"onPanelModeChange($event)\"\n              [value]=\"nzValue\"\n              (chooseYear)=\"onChooseValue('year', $event)\"\n              (chooseMonth)=\"onChooseValue('month', $event)\"\n              [locale]=\"nzLocale.lang\"\n              [enablePrev]=\"true\"\n              [enableNext]=\"true\"\n            ></calendar-header>\n          </div>\n          <calendar-footer *ngIf=\"extraFooter\" [extraFooter]=\"extraFooter\"></calendar-footer>\n        </div>\n      </div>\n    </div>\n  </div>\n</nz-picker>",
                    providers: [
                        {
                            provide: NG_VALUE_ACCESSOR,
                            multi: true,
                            useExisting: forwardRef((/**
                             * @return {?}
                             */
                            function () { return NzMonthPickerComponent; }))
                        }
                    ]
                }] }
    ];
    /** @nocollapse */
    NzMonthPickerComponent.ctorParameters = function () { return [
        { type: NzI18nService },
        { type: ChangeDetectorRef },
        { type: DateHelperService },
        { type: Renderer2 },
        { type: ElementRef },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzMonthPickerComponent.propDecorators = {
        nzFormat: [{ type: Input }]
    };
    return NzMonthPickerComponent;
}(HeaderPickerComponent));
export { NzMonthPickerComponent };
if (false) {
    /** @type {?} */
    NzMonthPickerComponent.prototype.nzFormat;
    /** @type {?} */
    NzMonthPickerComponent.prototype.endPanelMode;
    /** @type {?} */
    NzMonthPickerComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibW9udGgtcGlja2VyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9tb250aC1waWNrZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsT0FBTyxFQUNMLFVBQVUsRUFDVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsSUFBSSxFQUNKLEtBQUssRUFDTCxRQUFRLEVBQ1IsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUVuRCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUN4RixPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFFeEQsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFDaEUsT0FBTyxFQUFFLHFCQUFxQixFQUFzQixNQUFNLDJCQUEyQixDQUFDO0FBRXRGO0lBYTRDLGtEQUFxQjtJQUsvRCxnQ0FDRSxJQUFtQixFQUNuQixHQUFzQixFQUN0QixVQUE2QixFQUM3QixRQUFtQixFQUNuQixVQUFzQixFQUNLLFdBQW9DO1FBTmpFLFlBUUUsa0JBQU0sSUFBSSxFQUFFLEdBQUcsRUFBRSxVQUFVLEVBQUUsV0FBVyxDQUFDLFNBRTFDO1FBSjRCLGlCQUFXLEdBQVgsV0FBVyxDQUF5QjtRQVZ4RCxjQUFRLEdBQVcsU0FBUyxDQUFDO1FBRXRDLGtCQUFZLEdBQXVCLE9BQU8sQ0FBQztRQVd6QyxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUscUJBQXFCLENBQUMsQ0FBQzs7SUFDckUsQ0FBQzs7Z0JBNUJGLFNBQVMsU0FBQztvQkFDVCxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07b0JBQy9DLFFBQVEsRUFBRSxpQkFBaUI7b0JBQzNCLCtpREFBNkM7b0JBQzdDLFNBQVMsRUFBRTt3QkFDVDs0QkFDRSxPQUFPLEVBQUUsaUJBQWlCOzRCQUMxQixLQUFLLEVBQUUsSUFBSTs0QkFDWCxXQUFXLEVBQUUsVUFBVTs7OzRCQUFDLGNBQU0sT0FBQSxzQkFBc0IsRUFBdEIsQ0FBc0IsRUFBQzt5QkFDdEQ7cUJBQ0Y7aUJBQ0Y7Ozs7Z0JBakJRLGFBQWE7Z0JBWnBCLGlCQUFpQjtnQkFjVixpQkFBaUI7Z0JBUnhCLFNBQVM7Z0JBSlQsVUFBVTtnQkFTSCxzQkFBc0IsdUJBOEIxQixJQUFJLFlBQUksUUFBUTs7OzJCQVZsQixLQUFLOztJQWVSLDZCQUFDO0NBQUEsQUE3QkQsQ0FhNEMscUJBQXFCLEdBZ0JoRTtTQWhCWSxzQkFBc0I7OztJQUNqQywwQ0FBc0M7O0lBRXRDLDhDQUEyQzs7SUFRekMsNkNBQStEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgZm9yd2FyZFJlZixcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEhvc3QsXG4gIElucHV0LFxuICBPcHRpb25hbCxcbiAgUmVuZGVyZXIyLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE5HX1ZBTFVFX0FDQ0VTU09SIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOekkxOG5TZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuXG5pbXBvcnQgeyBEYXRlSGVscGVyU2VydmljZSB9IGZyb20gJy4uL2kxOG4vZGF0ZS1oZWxwZXIuc2VydmljZSc7XG5pbXBvcnQgeyBIZWFkZXJQaWNrZXJDb21wb25lbnQsIFN1cHBvcnRIZWFkZXJQYW5lbCB9IGZyb20gJy4vaGVhZGVyLXBpY2tlci5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIHNlbGVjdG9yOiAnbnotbW9udGgtcGlja2VyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL2hlYWRlci1waWNrZXIuY29tcG9uZW50Lmh0bWwnLFxuICBwcm92aWRlcnM6IFtcbiAgICB7XG4gICAgICBwcm92aWRlOiBOR19WQUxVRV9BQ0NFU1NPUixcbiAgICAgIG11bHRpOiB0cnVlLFxuICAgICAgdXNlRXhpc3Rpbmc6IGZvcndhcmRSZWYoKCkgPT4gTnpNb250aFBpY2tlckNvbXBvbmVudClcbiAgICB9XG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpNb250aFBpY2tlckNvbXBvbmVudCBleHRlbmRzIEhlYWRlclBpY2tlckNvbXBvbmVudCB7XG4gIEBJbnB1dCgpIG56Rm9ybWF0OiBzdHJpbmcgPSAneXl5eS1NTSc7XG5cbiAgZW5kUGFuZWxNb2RlOiBTdXBwb3J0SGVhZGVyUGFuZWwgPSAnbW9udGgnO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGkxOG46IE56STE4blNlcnZpY2UsXG4gICAgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSxcbiAgICByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgQEhvc3QoKSBAT3B0aW9uYWwoKSBwdWJsaWMgbm9BbmltYXRpb24/OiBOek5vQW5pbWF0aW9uRGlyZWN0aXZlXG4gICkge1xuICAgIHN1cGVyKGkxOG4sIGNkciwgZGF0ZUhlbHBlciwgbm9BbmltYXRpb24pO1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1jYWxlbmRhci1waWNrZXInKTtcbiAgfVxufVxuIl19