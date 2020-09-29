/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { slideAlertMotion } from '../core/animation/slide';
import { InputBoolean } from '../core/util/convert';
export class NzAlertComponent {
    constructor() {
        this.destroy = false;
        this.iconType = 'info-circle';
        this.iconTheme = 'fill';
        this.isTypeSet = false;
        this.isShowIconSet = false;
        this.nzType = 'info';
        this.nzCloseable = false;
        this.nzShowIcon = false;
        this.nzBanner = false;
        this.nzOnClose = new EventEmitter();
    }
    /**
     * @return {?}
     */
    closeAlert() {
        this.destroy = true;
    }
    /**
     * @return {?}
     */
    onFadeAnimationDone() {
        if (this.destroy) {
            this.nzOnClose.emit(true);
        }
    }
    /**
     * @return {?}
     */
    updateIconClassMap() {
        switch (this.nzType) {
            case 'error':
                this.iconType = 'close-circle';
                break;
            case 'success':
                this.iconType = 'check-circle';
                break;
            case 'info':
                this.iconType = 'info-circle';
                break;
            case 'warning':
                this.iconType = 'exclamation-circle';
                break;
        }
        this.iconTheme = this.nzDescription ? 'outline' : 'fill';
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        const { nzShowIcon, nzDescription, nzType, nzBanner } = changes;
        if (nzShowIcon) {
            this.isShowIconSet = true;
        }
        if (nzDescription || nzType) {
            this.updateIconClassMap();
        }
        if (nzType) {
            this.isTypeSet = true;
        }
        if (nzBanner) {
            if (!this.isTypeSet) {
                this.nzType = 'warning';
            }
            if (!this.isShowIconSet) {
                this.nzShowIcon = true;
            }
        }
    }
}
NzAlertComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-alert',
                animations: [slideAlertMotion],
                template: "<div *ngIf=\"!destroy\"\n  class=\"ant-alert\"\n  [class.ant-alert-success]=\"nzType === 'success'\"\n  [class.ant-alert-info]=\"nzType === 'info'\"\n  [class.ant-alert-warning]=\"nzType === 'warning'\"\n  [class.ant-alert-error]=\"nzType === 'error'\"\n  [class.ant-alert-no-icon]=\"!nzShowIcon\"\n  [class.ant-alert-banner]=\"nzBanner\"\n  [class.ant-alert-closable]=\"nzCloseable\"\n  [class.ant-alert-with-description]=\"!!nzDescription\"\n  [@slideAlertMotion]\n  (@slideAlertMotion.done)=\"onFadeAnimationDone()\">\n  <ng-container *ngIf=\"nzShowIcon\">\n    <i class=\"ant-alert-icon\" [ngClass]=\"nzIconType\" *ngIf=\"nzIconType; else iconTemplate\"></i>\n    <ng-template #iconTemplate>\n      <i nz-icon class=\"ant-alert-icon\" [type]=\"iconType\" [theme]=\"iconTheme\"></i>\n    </ng-template>\n  </ng-container>\n  <span class=\"ant-alert-message\" *ngIf=\"nzMessage\">\n    <ng-container *nzStringTemplateOutlet=\"nzMessage\">{{ nzMessage }}</ng-container>\n  </span>\n  <span class=\"ant-alert-description\" *ngIf=\"nzDescription\">\n    <ng-container *nzStringTemplateOutlet=\"nzDescription\">{{ nzDescription }}</ng-container>\n  </span>\n  <a *ngIf=\"nzCloseable || nzCloseText\"\n    class=\"ant-alert-close-icon\"\n    (click)=\"closeAlert()\">\n    <ng-template #closeDefaultTemplate>\n      <i nz-icon type=\"close\" class=\"anticon-close\"></i>\n    </ng-template>\n    <ng-container *ngIf=\"nzCloseText; else closeDefaultTemplate\">\n      <ng-container *nzStringTemplateOutlet=\"nzCloseText\">{{ nzCloseText }}</ng-container>\n    </ng-container>\n  </a>\n</div>",
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                styles: [`
      nz-alert {
        display: block;
      }
    `]
            }] }
];
NzAlertComponent.propDecorators = {
    nzCloseText: [{ type: Input }],
    nzIconType: [{ type: Input }],
    nzMessage: [{ type: Input }],
    nzDescription: [{ type: Input }],
    nzType: [{ type: Input }],
    nzCloseable: [{ type: Input }],
    nzShowIcon: [{ type: Input }],
    nzBanner: [{ type: Input }],
    nzOnClose: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzAlertComponent.prototype, "nzCloseable", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzAlertComponent.prototype, "nzShowIcon", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzAlertComponent.prototype, "nzBanner", void 0);
if (false) {
    /** @type {?} */
    NzAlertComponent.prototype.destroy;
    /** @type {?} */
    NzAlertComponent.prototype.iconType;
    /** @type {?} */
    NzAlertComponent.prototype.iconTheme;
    /**
     * @type {?}
     * @private
     */
    NzAlertComponent.prototype.isTypeSet;
    /**
     * @type {?}
     * @private
     */
    NzAlertComponent.prototype.isShowIconSet;
    /** @type {?} */
    NzAlertComponent.prototype.nzCloseText;
    /** @type {?} */
    NzAlertComponent.prototype.nzIconType;
    /** @type {?} */
    NzAlertComponent.prototype.nzMessage;
    /** @type {?} */
    NzAlertComponent.prototype.nzDescription;
    /** @type {?} */
    NzAlertComponent.prototype.nzType;
    /** @type {?} */
    NzAlertComponent.prototype.nzCloseable;
    /** @type {?} */
    NzAlertComponent.prototype.nzShowIcon;
    /** @type {?} */
    NzAlertComponent.prototype.nzBanner;
    /** @type {?} */
    NzAlertComponent.prototype.nzOnClose;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYWxlcnQuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImFsZXJ0L256LWFsZXJ0LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFlBQVksRUFDWixLQUFLLEVBRUwsTUFBTSxFQUdOLGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUUzRCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFpQnBELE1BQU0sT0FBTyxnQkFBZ0I7SUFmN0I7UUFnQkUsWUFBTyxHQUFHLEtBQUssQ0FBQztRQUNoQixhQUFRLEdBQUcsYUFBYSxDQUFDO1FBQ3pCLGNBQVMsR0FBRyxNQUFNLENBQUM7UUFDWCxjQUFTLEdBQUcsS0FBSyxDQUFDO1FBQ2xCLGtCQUFhLEdBQUcsS0FBSyxDQUFDO1FBS3JCLFdBQU0sR0FBNkMsTUFBTSxDQUFDO1FBQzFDLGdCQUFXLEdBQUcsS0FBSyxDQUFDO1FBQ3BCLGVBQVUsR0FBRyxLQUFLLENBQUM7UUFDbkIsYUFBUSxHQUFHLEtBQUssQ0FBQztRQUN2QixjQUFTLEdBQUcsSUFBSSxZQUFZLEVBQVcsQ0FBQztJQWtEN0QsQ0FBQzs7OztJQWhEQyxVQUFVO1FBQ1IsSUFBSSxDQUFDLE9BQU8sR0FBRyxJQUFJLENBQUM7SUFDdEIsQ0FBQzs7OztJQUVELG1CQUFtQjtRQUNqQixJQUFJLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDaEIsSUFBSSxDQUFDLFNBQVMsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDM0I7SUFDSCxDQUFDOzs7O0lBRUQsa0JBQWtCO1FBQ2hCLFFBQVEsSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNuQixLQUFLLE9BQU87Z0JBQ1YsSUFBSSxDQUFDLFFBQVEsR0FBRyxjQUFjLENBQUM7Z0JBQy9CLE1BQU07WUFDUixLQUFLLFNBQVM7Z0JBQ1osSUFBSSxDQUFDLFFBQVEsR0FBRyxjQUFjLENBQUM7Z0JBQy9CLE1BQU07WUFDUixLQUFLLE1BQU07Z0JBQ1QsSUFBSSxDQUFDLFFBQVEsR0FBRyxhQUFhLENBQUM7Z0JBQzlCLE1BQU07WUFDUixLQUFLLFNBQVM7Z0JBQ1osSUFBSSxDQUFDLFFBQVEsR0FBRyxvQkFBb0IsQ0FBQztnQkFDckMsTUFBTTtTQUNUO1FBQ0QsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUMsYUFBYSxDQUFDLENBQUMsQ0FBQyxTQUFTLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FBQztJQUMzRCxDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtjQUMxQixFQUFFLFVBQVUsRUFBRSxhQUFhLEVBQUUsTUFBTSxFQUFFLFFBQVEsRUFBRSxHQUFHLE9BQU87UUFDL0QsSUFBSSxVQUFVLEVBQUU7WUFDZCxJQUFJLENBQUMsYUFBYSxHQUFHLElBQUksQ0FBQztTQUMzQjtRQUNELElBQUksYUFBYSxJQUFJLE1BQU0sRUFBRTtZQUMzQixJQUFJLENBQUMsa0JBQWtCLEVBQUUsQ0FBQztTQUMzQjtRQUNELElBQUksTUFBTSxFQUFFO1lBQ1YsSUFBSSxDQUFDLFNBQVMsR0FBRyxJQUFJLENBQUM7U0FDdkI7UUFDRCxJQUFJLFFBQVEsRUFBRTtZQUNaLElBQUksQ0FBQyxJQUFJLENBQUMsU0FBUyxFQUFFO2dCQUNuQixJQUFJLENBQUMsTUFBTSxHQUFHLFNBQVMsQ0FBQzthQUN6QjtZQUNELElBQUksQ0FBQyxJQUFJLENBQUMsYUFBYSxFQUFFO2dCQUN2QixJQUFJLENBQUMsVUFBVSxHQUFHLElBQUksQ0FBQzthQUN4QjtTQUNGO0lBQ0gsQ0FBQzs7O1lBOUVGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsVUFBVTtnQkFDcEIsVUFBVSxFQUFFLENBQUMsZ0JBQWdCLENBQUM7Z0JBQzlCLCtqREFBd0M7Z0JBQ3hDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsbUJBQW1CLEVBQUUsS0FBSzt5QkFFeEI7Ozs7S0FJQzthQUVKOzs7MEJBT0UsS0FBSzt5QkFDTCxLQUFLO3dCQUNMLEtBQUs7NEJBQ0wsS0FBSztxQkFDTCxLQUFLOzBCQUNMLEtBQUs7eUJBQ0wsS0FBSzt1QkFDTCxLQUFLO3dCQUNMLE1BQU07O0FBSGtCO0lBQWYsWUFBWSxFQUFFOztxREFBcUI7QUFDcEI7SUFBZixZQUFZLEVBQUU7O29EQUFvQjtBQUNuQjtJQUFmLFlBQVksRUFBRTs7a0RBQWtCOzs7SUFaMUMsbUNBQWdCOztJQUNoQixvQ0FBeUI7O0lBQ3pCLHFDQUFtQjs7Ozs7SUFDbkIscUNBQTBCOzs7OztJQUMxQix5Q0FBOEI7O0lBQzlCLHVDQUFpRDs7SUFDakQsc0NBQWlDOztJQUNqQyxxQ0FBK0M7O0lBQy9DLHlDQUFtRDs7SUFDbkQsa0NBQW1FOztJQUNuRSx1Q0FBNkM7O0lBQzdDLHNDQUE0Qzs7SUFDNUMsb0NBQTBDOztJQUMxQyxxQ0FBMkQiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE91dHB1dCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgc2xpZGVBbGVydE1vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL3NsaWRlJztcbmltcG9ydCB7IE5nQ2xhc3NUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9uZy1jbGFzcyc7XG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWFsZXJ0JyxcbiAgYW5pbWF0aW9uczogW3NsaWRlQWxlcnRNb3Rpb25dLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotYWxlcnQuY29tcG9uZW50Lmh0bWwnLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHN0eWxlczogW1xuICAgIGBcbiAgICAgIG56LWFsZXJ0IHtcbiAgICAgICAgZGlzcGxheTogYmxvY2s7XG4gICAgICB9XG4gICAgYFxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56QWxlcnRDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMge1xuICBkZXN0cm95ID0gZmFsc2U7XG4gIGljb25UeXBlID0gJ2luZm8tY2lyY2xlJztcbiAgaWNvblRoZW1lID0gJ2ZpbGwnO1xuICBwcml2YXRlIGlzVHlwZVNldCA9IGZhbHNlO1xuICBwcml2YXRlIGlzU2hvd0ljb25TZXQgPSBmYWxzZTtcbiAgQElucHV0KCkgbnpDbG9zZVRleHQ6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuekljb25UeXBlOiBOZ0NsYXNzVHlwZTtcbiAgQElucHV0KCkgbnpNZXNzYWdlOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpEZXNjcmlwdGlvbjogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56VHlwZTogJ3N1Y2Nlc3MnIHwgJ2luZm8nIHwgJ3dhcm5pbmcnIHwgJ2Vycm9yJyA9ICdpbmZvJztcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Q2xvc2VhYmxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dJY29uID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekJhbm5lciA9IGZhbHNlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpPbkNsb3NlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuXG4gIGNsb3NlQWxlcnQoKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95ID0gdHJ1ZTtcbiAgfVxuXG4gIG9uRmFkZUFuaW1hdGlvbkRvbmUoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuZGVzdHJveSkge1xuICAgICAgdGhpcy5uek9uQ2xvc2UuZW1pdCh0cnVlKTtcbiAgICB9XG4gIH1cblxuICB1cGRhdGVJY29uQ2xhc3NNYXAoKTogdm9pZCB7XG4gICAgc3dpdGNoICh0aGlzLm56VHlwZSkge1xuICAgICAgY2FzZSAnZXJyb3InOlxuICAgICAgICB0aGlzLmljb25UeXBlID0gJ2Nsb3NlLWNpcmNsZSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnc3VjY2Vzcyc6XG4gICAgICAgIHRoaXMuaWNvblR5cGUgPSAnY2hlY2stY2lyY2xlJztcbiAgICAgICAgYnJlYWs7XG4gICAgICBjYXNlICdpbmZvJzpcbiAgICAgICAgdGhpcy5pY29uVHlwZSA9ICdpbmZvLWNpcmNsZSc7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnd2FybmluZyc6XG4gICAgICAgIHRoaXMuaWNvblR5cGUgPSAnZXhjbGFtYXRpb24tY2lyY2xlJztcbiAgICAgICAgYnJlYWs7XG4gICAgfVxuICAgIHRoaXMuaWNvblRoZW1lID0gdGhpcy5uekRlc2NyaXB0aW9uID8gJ291dGxpbmUnIDogJ2ZpbGwnO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGNvbnN0IHsgbnpTaG93SWNvbiwgbnpEZXNjcmlwdGlvbiwgbnpUeXBlLCBuekJhbm5lciB9ID0gY2hhbmdlcztcbiAgICBpZiAobnpTaG93SWNvbikge1xuICAgICAgdGhpcy5pc1Nob3dJY29uU2V0ID0gdHJ1ZTtcbiAgICB9XG4gICAgaWYgKG56RGVzY3JpcHRpb24gfHwgbnpUeXBlKSB7XG4gICAgICB0aGlzLnVwZGF0ZUljb25DbGFzc01hcCgpO1xuICAgIH1cbiAgICBpZiAobnpUeXBlKSB7XG4gICAgICB0aGlzLmlzVHlwZVNldCA9IHRydWU7XG4gICAgfVxuICAgIGlmIChuekJhbm5lcikge1xuICAgICAgaWYgKCF0aGlzLmlzVHlwZVNldCkge1xuICAgICAgICB0aGlzLm56VHlwZSA9ICd3YXJuaW5nJztcbiAgICAgIH1cbiAgICAgIGlmICghdGhpcy5pc1Nob3dJY29uU2V0KSB7XG4gICAgICAgIHRoaXMubnpTaG93SWNvbiA9IHRydWU7XG4gICAgICB9XG4gICAgfVxuICB9XG59XG4iXX0=