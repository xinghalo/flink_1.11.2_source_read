/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectorRef, Component, Input } from '@angular/core';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { valueFunctionProp } from '../core/util/convert';
import { DateHelperService } from '../i18n/date-helper.service';
import { NzI18nService } from '../i18n/nz-i18n.service';
import { AbstractPickerComponent } from './abstract-picker.component';
import { CandyDate } from './lib/candy-date';
/**
 * The base picker for header panels, current support: Year/Month
 */
export class HeaderPickerComponent extends AbstractPickerComponent {
    /**
     * @param {?} i18n
     * @param {?} cdr
     * @param {?} dateHelper
     * @param {?=} noAnimation
     */
    constructor(i18n, cdr, dateHelper, noAnimation) {
        super(i18n, cdr, dateHelper, noAnimation);
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        super.ngOnInit();
        this.panelMode = this.endPanelMode;
        /** @type {?} */
        const allHeaderPanels = ['decade', 'year', 'month'];
        this.supportPanels = allHeaderPanels.slice(0, allHeaderPanels.indexOf(this.endPanelMode) + 1);
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
    }
    /**
     * @param {?} mode
     * @return {?}
     */
    onPanelModeChange(mode) {
        if (this.supportPanels.indexOf(mode) > -1) {
            this.panelMode = mode;
        }
        else {
            // Since the default "click year" logic can be "year panel" -> "date panel", we need force to the end panel otherwise
            this.panelMode = this.endPanelMode;
        }
    }
    /**
     * @param {?} mode
     * @param {?} value
     * @return {?}
     */
    onChooseValue(mode, value) {
        if (this.endPanelMode === mode) {
            super.onValueChange(value);
            this.closeOverlay();
        }
    }
    /**
     * @param {?} open
     * @return {?}
     */
    onOpenChange(open) {
        if (!open) {
            this.cleanUp();
        }
        this.nzOnOpenChange.emit(open);
    }
    // Restore some initial props to let open as new in next time
    /**
     * @private
     * @return {?}
     */
    cleanUp() {
        this.panelMode = this.endPanelMode;
    }
}
HeaderPickerComponent.decorators = [
    { type: Component, args: [{
                template: ``
            }] }
];
/** @nocollapse */
HeaderPickerComponent.ctorParameters = () => [
    { type: NzI18nService },
    { type: ChangeDetectorRef },
    { type: DateHelperService },
    { type: NzNoAnimationDirective }
];
HeaderPickerComponent.propDecorators = {
    nzPlaceHolder: [{ type: Input }],
    nzRenderExtraFooter: [{ type: Input }],
    nzDefaultValue: [{ type: Input }],
    nzFormat: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    HeaderPickerComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    HeaderPickerComponent.prototype.nzRenderExtraFooter;
    /** @type {?} */
    HeaderPickerComponent.prototype.nzDefaultValue;
    /** @type {?} */
    HeaderPickerComponent.prototype.nzFormat;
    /** @type {?} */
    HeaderPickerComponent.prototype.endPanelMode;
    /** @type {?} */
    HeaderPickerComponent.prototype.panelMode;
    /** @type {?} */
    HeaderPickerComponent.prototype.extraFooter;
    /**
     * @type {?}
     * @private
     */
    HeaderPickerComponent.prototype.supportPanels;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaGVhZGVyLXBpY2tlci5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZGF0ZS1waWNrZXIvaGVhZGVyLXBpY2tlci5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxTQUFTLEVBQUUsS0FBSyxFQUFpRCxNQUFNLGVBQWUsQ0FBQztBQUVuSCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUV4RixPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUN6RCxPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSw2QkFBNkIsQ0FBQztBQUNoRSxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFDeEQsT0FBTyxFQUFFLHVCQUF1QixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFDdEUsT0FBTyxFQUFFLFNBQVMsRUFBRSxNQUFNLGtCQUFrQixDQUFDOzs7O0FBVTdDLE1BQU0sT0FBTyxxQkFBc0IsU0FBUSx1QkFBdUI7Ozs7Ozs7SUFhaEUsWUFDRSxJQUFtQixFQUNuQixHQUFzQixFQUN0QixVQUE2QixFQUM3QixXQUFvQztRQUVwQyxLQUFLLENBQUMsSUFBSSxFQUFFLEdBQUcsRUFBRSxVQUFVLEVBQUUsV0FBVyxDQUFDLENBQUM7SUFDNUMsQ0FBQzs7OztJQUVELFFBQVE7UUFDTixLQUFLLENBQUMsUUFBUSxFQUFFLENBQUM7UUFFakIsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDOztjQUU3QixlQUFlLEdBQWdCLENBQUMsUUFBUSxFQUFFLE1BQU0sRUFBRSxPQUFPLENBQUM7UUFDaEUsSUFBSSxDQUFDLGFBQWEsR0FBRyxlQUFlLENBQUMsS0FBSyxDQUFDLENBQUMsRUFBRSxlQUFlLENBQUMsT0FBTyxDQUFDLElBQUksQ0FBQyxZQUFZLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQztJQUNoRyxDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxLQUFLLENBQUMsV0FBVyxDQUFDLE9BQU8sQ0FBQyxDQUFDO1FBRTNCLElBQUksT0FBTyxDQUFDLG1CQUFtQixFQUFFO1lBQy9CLElBQUksQ0FBQyxXQUFXLEdBQUcsaUJBQWlCLENBQUMsSUFBSSxDQUFDLG1CQUFtQixDQUFDLENBQUM7U0FDaEU7SUFDSCxDQUFDOzs7OztJQUVELGlCQUFpQixDQUFDLElBQWU7UUFDL0IsSUFBSSxJQUFJLENBQUMsYUFBYSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBRTtZQUN6QyxJQUFJLENBQUMsU0FBUyxHQUFHLElBQUksQ0FBQztTQUN2QjthQUFNO1lBQ0wscUhBQXFIO1lBQ3JILElBQUksQ0FBQyxTQUFTLEdBQUcsSUFBSSxDQUFDLFlBQVksQ0FBQztTQUNwQztJQUNILENBQUM7Ozs7OztJQUVELGFBQWEsQ0FBQyxJQUF3QixFQUFFLEtBQWdCO1FBQ3RELElBQUksSUFBSSxDQUFDLFlBQVksS0FBSyxJQUFJLEVBQUU7WUFDOUIsS0FBSyxDQUFDLGFBQWEsQ0FBQyxLQUFLLENBQUMsQ0FBQztZQUUzQixJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7U0FDckI7SUFDSCxDQUFDOzs7OztJQUVELFlBQVksQ0FBQyxJQUFhO1FBQ3hCLElBQUksQ0FBQyxJQUFJLEVBQUU7WUFDVCxJQUFJLENBQUMsT0FBTyxFQUFFLENBQUM7U0FDaEI7UUFDRCxJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNqQyxDQUFDOzs7Ozs7SUFHTyxPQUFPO1FBQ2IsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUMsWUFBWSxDQUFDO0lBQ3JDLENBQUM7OztZQXJFRixTQUFTLFNBQUM7Z0JBQ1QsUUFBUSxFQUFFLEVBQUU7YUFDYjs7OztZQVhRLGFBQWE7WUFOYixpQkFBaUI7WUFLakIsaUJBQWlCO1lBSGpCLHNCQUFzQjs7OzRCQWlCNUIsS0FBSztrQ0FFTCxLQUFLOzZCQUNMLEtBQUs7dUJBQ0wsS0FBSzs7OztJQUpOLDhDQUErQjs7SUFFL0Isb0RBQXVFOztJQUN2RSwrQ0FBbUM7O0lBQ25DLHlDQUEwQjs7SUFFMUIsNkNBQWlDOztJQUNqQywwQ0FBcUI7O0lBQ3JCLDRDQUF3Qzs7Ozs7SUFFeEMsOENBQW1DIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0b3JSZWYsIENvbXBvbmVudCwgSW5wdXQsIE9uQ2hhbmdlcywgT25Jbml0LCBTaW1wbGVDaGFuZ2VzLCBUZW1wbGF0ZVJlZiB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBGdW5jdGlvblByb3AgfSBmcm9tICcuLi9jb3JlL3R5cGVzL2NvbW1vbi13cmFwJztcbmltcG9ydCB7IHZhbHVlRnVuY3Rpb25Qcm9wIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgRGF0ZUhlbHBlclNlcnZpY2UgfSBmcm9tICcuLi9pMThuL2RhdGUtaGVscGVyLnNlcnZpY2UnO1xuaW1wb3J0IHsgTnpJMThuU2VydmljZSB9IGZyb20gJy4uL2kxOG4vbnotaTE4bi5zZXJ2aWNlJztcbmltcG9ydCB7IEFic3RyYWN0UGlja2VyQ29tcG9uZW50IH0gZnJvbSAnLi9hYnN0cmFjdC1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4vbGliL2NhbmR5LWRhdGUnO1xuaW1wb3J0IHsgUGFuZWxNb2RlIH0gZnJvbSAnLi9zdGFuZGFyZC10eXBlcyc7XG5cbi8qKlxuICogVGhlIGJhc2UgcGlja2VyIGZvciBoZWFkZXIgcGFuZWxzLCBjdXJyZW50IHN1cHBvcnQ6IFllYXIvTW9udGhcbiAqL1xuXG5AQ29tcG9uZW50KHtcbiAgdGVtcGxhdGU6IGBgXG59KVxuZXhwb3J0IGNsYXNzIEhlYWRlclBpY2tlckNvbXBvbmVudCBleHRlbmRzIEFic3RyYWN0UGlja2VyQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBuelBsYWNlSG9sZGVyOiBzdHJpbmc7XG5cbiAgQElucHV0KCkgbnpSZW5kZXJFeHRyYUZvb3RlcjogRnVuY3Rpb25Qcm9wPFRlbXBsYXRlUmVmPHZvaWQ+IHwgc3RyaW5nPjtcbiAgQElucHV0KCkgbnpEZWZhdWx0VmFsdWU6IENhbmR5RGF0ZTtcbiAgQElucHV0KCkgbnpGb3JtYXQ6IHN0cmluZzsgLy8gW0Nhbm1wbGVtZW50ZWQgYnkgc3ViIGNsYXNzXSBUaGUgb3V0cHV0IGZvcm1hdFxuXG4gIGVuZFBhbmVsTW9kZTogU3VwcG9ydEhlYWRlclBhbmVsOyAvLyBbSW1wbGVtZW50ZWQgYnkgc3ViIGNsYXNzXSBUaGUgZmluYWwgcGFuZWwgZm9yIHBpY2tpbmcgYSBkYXRlXG4gIHBhbmVsTW9kZTogUGFuZWxNb2RlOyAvLyBDdXJyZW50IHBhbmVsIG1vZGVcbiAgZXh0cmFGb290ZXI6IFRlbXBsYXRlUmVmPHZvaWQ+IHwgc3RyaW5nO1xuXG4gIHByaXZhdGUgc3VwcG9ydFBhbmVsczogUGFuZWxNb2RlW107XG5cbiAgY29uc3RydWN0b3IoXG4gICAgaTE4bjogTnpJMThuU2VydmljZSxcbiAgICBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIGRhdGVIZWxwZXI6IERhdGVIZWxwZXJTZXJ2aWNlLFxuICAgIG5vQW5pbWF0aW9uPzogTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZVxuICApIHtcbiAgICBzdXBlcihpMThuLCBjZHIsIGRhdGVIZWxwZXIsIG5vQW5pbWF0aW9uKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIHN1cGVyLm5nT25Jbml0KCk7XG5cbiAgICB0aGlzLnBhbmVsTW9kZSA9IHRoaXMuZW5kUGFuZWxNb2RlO1xuXG4gICAgY29uc3QgYWxsSGVhZGVyUGFuZWxzOiBQYW5lbE1vZGVbXSA9IFsnZGVjYWRlJywgJ3llYXInLCAnbW9udGgnXTtcbiAgICB0aGlzLnN1cHBvcnRQYW5lbHMgPSBhbGxIZWFkZXJQYW5lbHMuc2xpY2UoMCwgYWxsSGVhZGVyUGFuZWxzLmluZGV4T2YodGhpcy5lbmRQYW5lbE1vZGUpICsgMSk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgc3VwZXIubmdPbkNoYW5nZXMoY2hhbmdlcyk7XG5cbiAgICBpZiAoY2hhbmdlcy5uelJlbmRlckV4dHJhRm9vdGVyKSB7XG4gICAgICB0aGlzLmV4dHJhRm9vdGVyID0gdmFsdWVGdW5jdGlvblByb3AodGhpcy5uelJlbmRlckV4dHJhRm9vdGVyKTtcbiAgICB9XG4gIH1cblxuICBvblBhbmVsTW9kZUNoYW5nZShtb2RlOiBQYW5lbE1vZGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5zdXBwb3J0UGFuZWxzLmluZGV4T2YobW9kZSkgPiAtMSkge1xuICAgICAgdGhpcy5wYW5lbE1vZGUgPSBtb2RlO1xuICAgIH0gZWxzZSB7XG4gICAgICAvLyBTaW5jZSB0aGUgZGVmYXVsdCBcImNsaWNrIHllYXJcIiBsb2dpYyBjYW4gYmUgXCJ5ZWFyIHBhbmVsXCIgLT4gXCJkYXRlIHBhbmVsXCIsIHdlIG5lZWQgZm9yY2UgdG8gdGhlIGVuZCBwYW5lbCBvdGhlcndpc2VcbiAgICAgIHRoaXMucGFuZWxNb2RlID0gdGhpcy5lbmRQYW5lbE1vZGU7XG4gICAgfVxuICB9XG5cbiAgb25DaG9vc2VWYWx1ZShtb2RlOiBTdXBwb3J0SGVhZGVyUGFuZWwsIHZhbHVlOiBDYW5keURhdGUpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5lbmRQYW5lbE1vZGUgPT09IG1vZGUpIHtcbiAgICAgIHN1cGVyLm9uVmFsdWVDaGFuZ2UodmFsdWUpO1xuXG4gICAgICB0aGlzLmNsb3NlT3ZlcmxheSgpO1xuICAgIH1cbiAgfVxuXG4gIG9uT3BlbkNoYW5nZShvcGVuOiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKCFvcGVuKSB7XG4gICAgICB0aGlzLmNsZWFuVXAoKTtcbiAgICB9XG4gICAgdGhpcy5uek9uT3BlbkNoYW5nZS5lbWl0KG9wZW4pO1xuICB9XG5cbiAgLy8gUmVzdG9yZSBzb21lIGluaXRpYWwgcHJvcHMgdG8gbGV0IG9wZW4gYXMgbmV3IGluIG5leHQgdGltZVxuICBwcml2YXRlIGNsZWFuVXAoKTogdm9pZCB7XG4gICAgdGhpcy5wYW5lbE1vZGUgPSB0aGlzLmVuZFBhbmVsTW9kZTtcbiAgfVxufVxuXG5leHBvcnQgdHlwZSBTdXBwb3J0SGVhZGVyUGFuZWwgPSAneWVhcicgfCAnbW9udGgnO1xuIl19