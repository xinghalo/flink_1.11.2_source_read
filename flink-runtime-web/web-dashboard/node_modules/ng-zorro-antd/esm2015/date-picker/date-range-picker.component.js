/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectorRef, Component, EventEmitter, Input, Output } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { toBoolean, valueFunctionProp, InputBoolean } from '../core/util/convert';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { DateHelperService } from '../i18n/date-helper.service';
import { AbstractPickerComponent } from './abstract-picker.component';
export class DateRangePickerComponent extends AbstractPickerComponent {
    /**
     * @param {?} i18n
     * @param {?} cdr
     * @param {?} dateHelper
     * @param {?=} noAnimation
     */
    constructor(i18n, cdr, dateHelper, noAnimation) {
        super(i18n, cdr, dateHelper, noAnimation);
        this.showWeek = false; // Should show as week picker
        this.nzShowToday = true;
        this.nzOnPanelChange = new EventEmitter();
        this.nzOnOk = new EventEmitter();
    }
    /**
     * @return {?}
     */
    get nzShowTime() {
        return this._showTime;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzShowTime(value) {
        this._showTime = typeof value === 'object' ? value : toBoolean(value);
    }
    /**
     * @return {?}
     */
    get realShowToday() {
        // Range not support nzShowToday currently
        return !this.isRange && this.nzShowToday;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        super.ngOnInit();
        // Default format when it's empty
        if (!this.nzFormat) {
            if (this.showWeek) {
                this.nzFormat = this.dateHelper.relyOnDatePipe ? 'yyyy-ww' : 'YYYY-WW'; // Format for week
            }
            else {
                if (this.dateHelper.relyOnDatePipe) {
                    this.nzFormat = this.nzShowTime ? 'yyyy-MM-dd HH:mm:ss' : 'yyyy-MM-dd';
                }
                else {
                    this.nzFormat = this.nzShowTime ? 'YYYY-MM-DD HH:mm:ss' : 'YYYY-MM-DD';
                }
            }
        }
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        super.ngOnChanges(changes);
        if (changes.nzRenderExtraFooter) {
            this.extraFooter = valueFunctionProp(this.nzRenderExtraFooter);
        }
        if (changes.nzShowTime || changes.nzStyle) {
            this.setFixedPickerStyle();
        }
    }
    // If has no timepicker and the user select a date by date panel, then close picker
    /**
     * @param {?} value
     * @return {?}
     */
    onValueChange(value) {
        super.onValueChange(value);
        if (!this.nzShowTime) {
            this.closeOverlay();
        }
    }
    // Emitted when done with date selecting
    /**
     * @return {?}
     */
    onResultOk() {
        if (this.isRange) {
            /** @type {?} */
            const value = (/** @type {?} */ (this.nzValue));
            if (value.length) {
                this.nzOnOk.emit([value[0].nativeDate, value[1].nativeDate]);
            }
            else {
                this.nzOnOk.emit([]);
            }
        }
        else {
            if (this.nzValue) {
                this.nzOnOk.emit(((/** @type {?} */ (this.nzValue))).nativeDate);
            }
            else {
                this.nzOnOk.emit(null);
            }
        }
        this.closeOverlay();
    }
    /**
     * @param {?} open
     * @return {?}
     */
    onOpenChange(open) {
        this.nzOnOpenChange.emit(open);
    }
    // Setup fixed style for picker
    /**
     * @private
     * @return {?}
     */
    setFixedPickerStyle() {
        /** @type {?} */
        const showTimeFixes = {};
        if (this.nzShowTime) {
            showTimeFixes.width = this.isRange ? '350px' : '195px';
        }
        this.pickerStyle = Object.assign({}, showTimeFixes, this.nzStyle);
    }
}
DateRangePickerComponent.decorators = [
    { type: Component, args: [{
                template: `` // Just for rollup
            }] }
];
/** @nocollapse */
DateRangePickerComponent.ctorParameters = () => [
    { type: NzI18nService },
    { type: ChangeDetectorRef },
    { type: DateHelperService },
    { type: NzNoAnimationDirective }
];
DateRangePickerComponent.propDecorators = {
    nzDateRender: [{ type: Input }],
    nzDisabledTime: [{ type: Input }],
    nzRenderExtraFooter: [{ type: Input }],
    nzShowToday: [{ type: Input }],
    nzMode: [{ type: Input }],
    nzRanges: [{ type: Input }],
    nzOnPanelChange: [{ type: Output }],
    nzShowTime: [{ type: Input }],
    nzOnOk: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Boolean)
], DateRangePickerComponent.prototype, "nzShowToday", void 0);
if (false) {
    /** @type {?} */
    DateRangePickerComponent.prototype.showWeek;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzDateRender;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzDisabledTime;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzRenderExtraFooter;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzShowToday;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzMode;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzRanges;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzOnPanelChange;
    /**
     * @type {?}
     * @private
     */
    DateRangePickerComponent.prototype._showTime;
    /** @type {?} */
    DateRangePickerComponent.prototype.nzOnOk;
    /** @type {?} */
    DateRangePickerComponent.prototype.pickerStyle;
    /** @type {?} */
    DateRangePickerComponent.prototype.extraFooter;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZS1yYW5nZS1waWNrZXIuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImRhdGUtcGlja2VyL2RhdGUtcmFuZ2UtcGlja2VyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCxpQkFBaUIsRUFDakIsU0FBUyxFQUNULFlBQVksRUFDWixLQUFLLEVBR0wsTUFBTSxFQUdQLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLGdEQUFnRCxDQUFDO0FBRXhGLE9BQU8sRUFBRSxTQUFTLEVBQUUsaUJBQWlCLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDbEYsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBR3hELE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ2hFLE9BQU8sRUFBRSx1QkFBdUIsRUFBa0IsTUFBTSw2QkFBNkIsQ0FBQztBQU10RixNQUFNLE9BQU8sd0JBQXlCLFNBQVEsdUJBQXVCOzs7Ozs7O0lBNkJuRSxZQUNFLElBQW1CLEVBQ25CLEdBQXNCLEVBQ3RCLFVBQTZCLEVBQzdCLFdBQW9DO1FBRXBDLEtBQUssQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFLFVBQVUsRUFBRSxXQUFXLENBQUMsQ0FBQztRQWxDNUMsYUFBUSxHQUFZLEtBQUssQ0FBQyxDQUFDLDZCQUE2QjtRQUsvQixnQkFBVyxHQUFZLElBQUksQ0FBQztRQUdsQyxvQkFBZSxHQUFHLElBQUksWUFBWSxFQUEyQixDQUFDO1FBVTlELFdBQU0sR0FBRyxJQUFJLFlBQVksRUFBeUIsQ0FBQztJQWlCdEUsQ0FBQzs7OztJQXhCRCxJQUFhLFVBQVU7UUFDckIsT0FBTyxJQUFJLENBQUMsU0FBUyxDQUFDO0lBQ3hCLENBQUM7Ozs7O0lBQ0QsSUFBSSxVQUFVLENBQUMsS0FBdUI7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QixhQUFhLEdBQXVCLEVBQUU7UUFDNUMsSUFBSSxJQUFJLENBQUMsVUFBVSxFQUFFO1lBQ25CLGFBQWEsQ0FBQyxLQUFLLEdBQUcsSUFBSSxDQUFDLE9BQU8sQ0FBQyxDQUFDLENBQUMsT0FBTyxDQUFDLENBQUMsQ0FBQyxPQUFPLENBQUM7U0FDeEQ7UUFFRCxJQUFJLENBQUMsV0FBVyxxQkFBUSxhQUFhLEVBQUssSUFBSSxDQUFDLE9BQU8sQ0FBRSxDQUFDO0lBQzNELENBQUM7OztZQTlHRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLEVBQUUsQ0FBQyxrQkFBa0I7YUFDaEM7Ozs7WUFUUSxhQUFhO1lBZHBCLGlCQUFpQjtZQWlCVixpQkFBaUI7WUFOakIsc0JBQXNCOzs7MkJBZ0I1QixLQUFLOzZCQUNMLEtBQUs7a0NBQ0wsS0FBSzswQkFDTCxLQUFLO3FCQUNMLEtBQUs7dUJBQ0wsS0FBSzs4QkFDTCxNQUFNO3lCQUdOLEtBQUs7cUJBT0wsTUFBTTs7QUFia0I7SUFBZixZQUFZLEVBQUU7OzZEQUE2Qjs7O0lBTHJELDRDQUEwQjs7SUFFMUIsZ0RBQWdFOztJQUNoRSxrREFBd0M7O0lBQ3hDLHVEQUF1RTs7SUFDdkUsK0NBQXFEOztJQUNyRCwwQ0FBeUM7O0lBQ3pDLDRDQUE4Qzs7SUFDOUMsbURBQWlGOzs7OztJQUVqRiw2Q0FBb0M7O0lBUXBDLDBDQUFzRTs7SUFPdEUsK0NBQW9COztJQUNwQiwrQ0FBd0MiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBTaW1wbGVDaGFuZ2VzLFxuICBUZW1wbGF0ZVJlZlxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgRnVuY3Rpb25Qcm9wIH0gZnJvbSAnLi4vY29yZS90eXBlcy9jb21tb24td3JhcCc7XG5pbXBvcnQgeyB0b0Jvb2xlYW4sIHZhbHVlRnVuY3Rpb25Qcm9wLCBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBOekkxOG5TZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLnNlcnZpY2UnO1xuaW1wb3J0IHsgQ2FuZHlEYXRlIH0gZnJvbSAnLi9saWIvY2FuZHktZGF0ZSc7XG5cbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IEFic3RyYWN0UGlja2VyQ29tcG9uZW50LCBDb21wYXRpYmxlRGF0ZSB9IGZyb20gJy4vYWJzdHJhY3QtcGlja2VyLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBEaXNhYmxlZFRpbWVGbiwgUGFuZWxNb2RlLCBQcmVzZXRSYW5nZXMgfSBmcm9tICcuL3N0YW5kYXJkLXR5cGVzJztcblxuQENvbXBvbmVudCh7XG4gIHRlbXBsYXRlOiBgYCAvLyBKdXN0IGZvciByb2xsdXBcbn0pXG5leHBvcnQgY2xhc3MgRGF0ZVJhbmdlUGlja2VyQ29tcG9uZW50IGV4dGVuZHMgQWJzdHJhY3RQaWNrZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIE9uQ2hhbmdlcyB7XG4gIHNob3dXZWVrOiBib29sZWFuID0gZmFsc2U7IC8vIFNob3VsZCBzaG93IGFzIHdlZWsgcGlja2VyXG5cbiAgQElucHV0KCkgbnpEYXRlUmVuZGVyOiBGdW5jdGlvblByb3A8VGVtcGxhdGVSZWY8RGF0ZT4gfCBzdHJpbmc+O1xuICBASW5wdXQoKSBuekRpc2FibGVkVGltZTogRGlzYWJsZWRUaW1lRm47XG4gIEBJbnB1dCgpIG56UmVuZGVyRXh0cmFGb290ZXI6IEZ1bmN0aW9uUHJvcDxUZW1wbGF0ZVJlZjx2b2lkPiB8IHN0cmluZz47XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dUb2RheTogYm9vbGVhbiA9IHRydWU7XG4gIEBJbnB1dCgpIG56TW9kZTogUGFuZWxNb2RlIHwgUGFuZWxNb2RlW107XG4gIEBJbnB1dCgpIG56UmFuZ2VzOiBGdW5jdGlvblByb3A8UHJlc2V0UmFuZ2VzPjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25QYW5lbENoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8UGFuZWxNb2RlIHwgUGFuZWxNb2RlW10+KCk7XG5cbiAgcHJpdmF0ZSBfc2hvd1RpbWU6IG9iamVjdCB8IGJvb2xlYW47XG4gIEBJbnB1dCgpIGdldCBuelNob3dUaW1lKCk6IG9iamVjdCB8IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9zaG93VGltZTtcbiAgfVxuICBzZXQgbnpTaG93VGltZSh2YWx1ZTogb2JqZWN0IHwgYm9vbGVhbikge1xuICAgIHRoaXMuX3Nob3dUaW1lID0gdHlwZW9mIHZhbHVlID09PSAnb2JqZWN0JyA/IHZhbHVlIDogdG9Cb29sZWFuKHZhbHVlKTtcbiAgfVxuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uT2sgPSBuZXcgRXZlbnRFbWl0dGVyPENvbXBhdGlibGVEYXRlIHwgbnVsbD4oKTtcblxuICBnZXQgcmVhbFNob3dUb2RheSgpOiBib29sZWFuIHtcbiAgICAvLyBSYW5nZSBub3Qgc3VwcG9ydCBuelNob3dUb2RheSBjdXJyZW50bHlcbiAgICByZXR1cm4gIXRoaXMuaXNSYW5nZSAmJiB0aGlzLm56U2hvd1RvZGF5O1xuICB9XG5cbiAgcGlja2VyU3R5bGU6IG9iamVjdDsgLy8gRmluYWwgcGlja2VyIHN0eWxlIHRoYXQgY29udGFpbnMgd2lkdGggZml4IGNvcnJlY3Rpb25zIGV0Yy5cbiAgZXh0cmFGb290ZXI6IFRlbXBsYXRlUmVmPHZvaWQ+IHwgc3RyaW5nO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGkxOG46IE56STE4blNlcnZpY2UsXG4gICAgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSxcbiAgICBub0FuaW1hdGlvbj86IE56Tm9BbmltYXRpb25EaXJlY3RpdmVcbiAgKSB7XG4gICAgc3VwZXIoaTE4biwgY2RyLCBkYXRlSGVscGVyLCBub0FuaW1hdGlvbik7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICBzdXBlci5uZ09uSW5pdCgpO1xuXG4gICAgLy8gRGVmYXVsdCBmb3JtYXQgd2hlbiBpdCdzIGVtcHR5XG4gICAgaWYgKCF0aGlzLm56Rm9ybWF0KSB7XG4gICAgICBpZiAodGhpcy5zaG93V2Vlaykge1xuICAgICAgICB0aGlzLm56Rm9ybWF0ID0gdGhpcy5kYXRlSGVscGVyLnJlbHlPbkRhdGVQaXBlID8gJ3l5eXktd3cnIDogJ1lZWVktV1cnOyAvLyBGb3JtYXQgZm9yIHdlZWtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGlmICh0aGlzLmRhdGVIZWxwZXIucmVseU9uRGF0ZVBpcGUpIHtcbiAgICAgICAgICB0aGlzLm56Rm9ybWF0ID0gdGhpcy5uelNob3dUaW1lID8gJ3l5eXktTU0tZGQgSEg6bW06c3MnIDogJ3l5eXktTU0tZGQnO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMubnpGb3JtYXQgPSB0aGlzLm56U2hvd1RpbWUgPyAnWVlZWS1NTS1ERCBISDptbTpzcycgOiAnWVlZWS1NTS1ERCc7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgc3VwZXIubmdPbkNoYW5nZXMoY2hhbmdlcyk7XG5cbiAgICBpZiAoY2hhbmdlcy5uelJlbmRlckV4dHJhRm9vdGVyKSB7XG4gICAgICB0aGlzLmV4dHJhRm9vdGVyID0gdmFsdWVGdW5jdGlvblByb3AodGhpcy5uelJlbmRlckV4dHJhRm9vdGVyKTtcbiAgICB9XG5cbiAgICBpZiAoY2hhbmdlcy5uelNob3dUaW1lIHx8IGNoYW5nZXMubnpTdHlsZSkge1xuICAgICAgdGhpcy5zZXRGaXhlZFBpY2tlclN0eWxlKCk7XG4gICAgfVxuICB9XG5cbiAgLy8gSWYgaGFzIG5vIHRpbWVwaWNrZXIgYW5kIHRoZSB1c2VyIHNlbGVjdCBhIGRhdGUgYnkgZGF0ZSBwYW5lbCwgdGhlbiBjbG9zZSBwaWNrZXJcbiAgb25WYWx1ZUNoYW5nZSh2YWx1ZTogQ2FuZHlEYXRlKTogdm9pZCB7XG4gICAgc3VwZXIub25WYWx1ZUNoYW5nZSh2YWx1ZSk7XG5cbiAgICBpZiAoIXRoaXMubnpTaG93VGltZSkge1xuICAgICAgdGhpcy5jbG9zZU92ZXJsYXkoKTtcbiAgICB9XG4gIH1cblxuICAvLyBFbWl0dGVkIHdoZW4gZG9uZSB3aXRoIGRhdGUgc2VsZWN0aW5nXG4gIG9uUmVzdWx0T2soKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgY29uc3QgdmFsdWUgPSB0aGlzLm56VmFsdWUgYXMgQ2FuZHlEYXRlW107XG4gICAgICBpZiAodmFsdWUubGVuZ3RoKSB7XG4gICAgICAgIHRoaXMubnpPbk9rLmVtaXQoW3ZhbHVlWzBdLm5hdGl2ZURhdGUsIHZhbHVlWzFdLm5hdGl2ZURhdGVdKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMubnpPbk9rLmVtaXQoW10pO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAodGhpcy5uelZhbHVlKSB7XG4gICAgICAgIHRoaXMubnpPbk9rLmVtaXQoKHRoaXMubnpWYWx1ZSBhcyBDYW5keURhdGUpLm5hdGl2ZURhdGUpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5uek9uT2suZW1pdChudWxsKTtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5jbG9zZU92ZXJsYXkoKTtcbiAgfVxuXG4gIG9uT3BlbkNoYW5nZShvcGVuOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5uek9uT3BlbkNoYW5nZS5lbWl0KG9wZW4pO1xuICB9XG5cbiAgLy8gU2V0dXAgZml4ZWQgc3R5bGUgZm9yIHBpY2tlclxuICBwcml2YXRlIHNldEZpeGVkUGlja2VyU3R5bGUoKTogdm9pZCB7XG4gICAgY29uc3Qgc2hvd1RpbWVGaXhlczogeyB3aWR0aD86IHN0cmluZyB9ID0ge307XG4gICAgaWYgKHRoaXMubnpTaG93VGltZSkge1xuICAgICAgc2hvd1RpbWVGaXhlcy53aWR0aCA9IHRoaXMuaXNSYW5nZSA/ICczNTBweCcgOiAnMTk1cHgnO1xuICAgIH1cblxuICAgIHRoaXMucGlja2VyU3R5bGUgPSB7IC4uLnNob3dUaW1lRml4ZXMsIC4uLnRoaXMubnpTdHlsZSB9O1xuICB9XG59XG4iXX0=