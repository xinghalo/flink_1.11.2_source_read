/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ContentChild, ElementRef, Input, Renderer2, TemplateRef, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
import { NzCardTabComponent } from './nz-card-tab.component';
var NzCardComponent = /** @class */ (function () {
    function NzCardComponent(renderer, elementRef) {
        this.nzBordered = true;
        this.nzLoading = false;
        this.nzHoverable = false;
        this.nzActions = [];
        renderer.addClass(elementRef.nativeElement, 'ant-card');
    }
    NzCardComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-card',
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<div class=\"ant-card-head\" *ngIf=\"nzTitle || nzExtra || tab\">\n  <div class=\"ant-card-head-wrapper\">\n    <div class=\"ant-card-head-title\" *ngIf=\"nzTitle\">\n      <ng-container *nzStringTemplateOutlet=\"nzTitle\">{{ nzTitle }}</ng-container>\n    </div>\n    <div class=\"ant-card-extra\" *ngIf=\"nzExtra\">\n      <ng-container *nzStringTemplateOutlet=\"nzExtra\">{{ nzExtra }}</ng-container>\n    </div>\n  </div>\n  <ng-container *ngIf=\"tab\">\n    <ng-template [ngTemplateOutlet]=\"tab.template\"></ng-template>\n  </ng-container>\n</div>\n<div class=\"ant-card-cover\" *ngIf=\"nzCover\">\n  <ng-template [ngTemplateOutlet]=\"nzCover\"></ng-template>\n</div>\n<div class=\"ant-card-body\" [ngStyle]=\"nzBodyStyle\">\n  <ng-container *ngIf=\"!nzLoading\">\n    <ng-content></ng-content>\n  </ng-container>\n  <nz-card-loading *ngIf=\"nzLoading\"></nz-card-loading>\n</div>\n<ul class=\"ant-card-actions\" *ngIf=\"nzActions.length\">\n  <li *ngFor=\"let action of nzActions\" [style.width.%]=\"100 / nzActions.length\">\n    <span><ng-template [ngTemplateOutlet]=\"action\"></ng-template></span>\n  </li>\n</ul>",
                    host: {
                        '[class.ant-card-loading]': 'nzLoading',
                        '[class.ant-card-bordered]': 'nzBordered',
                        '[class.ant-card-hoverable]': 'nzHoverable',
                        '[class.ant-card-type-inner]': "nzType === 'inner'",
                        '[class.ant-card-contain-tabs]': '!!tab'
                    },
                    styles: ["\n      nz-card {\n        display: block;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzCardComponent.ctorParameters = function () { return [
        { type: Renderer2 },
        { type: ElementRef }
    ]; };
    NzCardComponent.propDecorators = {
        nzBordered: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzHoverable: [{ type: Input }],
        nzBodyStyle: [{ type: Input }],
        nzCover: [{ type: Input }],
        nzActions: [{ type: Input }],
        nzType: [{ type: Input }],
        nzTitle: [{ type: Input }],
        nzExtra: [{ type: Input }],
        tab: [{ type: ContentChild, args: [NzCardTabComponent,] }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCardComponent.prototype, "nzBordered", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCardComponent.prototype, "nzLoading", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCardComponent.prototype, "nzHoverable", void 0);
    return NzCardComponent;
}());
export { NzCardComponent };
if (false) {
    /** @type {?} */
    NzCardComponent.prototype.nzBordered;
    /** @type {?} */
    NzCardComponent.prototype.nzLoading;
    /** @type {?} */
    NzCardComponent.prototype.nzHoverable;
    /** @type {?} */
    NzCardComponent.prototype.nzBodyStyle;
    /** @type {?} */
    NzCardComponent.prototype.nzCover;
    /** @type {?} */
    NzCardComponent.prototype.nzActions;
    /** @type {?} */
    NzCardComponent.prototype.nzType;
    /** @type {?} */
    NzCardComponent.prototype.nzTitle;
    /** @type {?} */
    NzCardComponent.prototype.nzExtra;
    /** @type {?} */
    NzCardComponent.prototype.tab;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FyZC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY2FyZC9uei1jYXJkLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFlBQVksRUFDWixVQUFVLEVBQ1YsS0FBSyxFQUNMLFNBQVMsRUFDVCxXQUFXLEVBQ1gsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsa0JBQWtCLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUU3RDtJQWlDRSx5QkFBWSxRQUFtQixFQUFFLFVBQXNCO1FBWDlCLGVBQVUsR0FBRyxJQUFJLENBQUM7UUFDbEIsY0FBUyxHQUFHLEtBQUssQ0FBQztRQUNsQixnQkFBVyxHQUFHLEtBQUssQ0FBQztRQUdwQyxjQUFTLEdBQTZCLEVBQUUsQ0FBQztRQU9oRCxRQUFRLENBQUMsUUFBUSxDQUFDLFVBQVUsQ0FBQyxhQUFhLEVBQUUsVUFBVSxDQUFDLENBQUM7SUFDMUQsQ0FBQzs7Z0JBbkNGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsU0FBUztvQkFDbkIsbUJBQW1CLEVBQUUsS0FBSztvQkFDMUIsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07b0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO29CQUNyQyxnbkNBQXVDO29CQVF2QyxJQUFJLEVBQUU7d0JBQ0osMEJBQTBCLEVBQUUsV0FBVzt3QkFDdkMsMkJBQTJCLEVBQUUsWUFBWTt3QkFDekMsNEJBQTRCLEVBQUUsYUFBYTt3QkFDM0MsNkJBQTZCLEVBQUUsb0JBQW9CO3dCQUNuRCwrQkFBK0IsRUFBRSxPQUFPO3FCQUN6Qzs2QkFaQywyREFJQztpQkFTSjs7OztnQkEzQkMsU0FBUztnQkFGVCxVQUFVOzs7NkJBK0JULEtBQUs7NEJBQ0wsS0FBSzs4QkFDTCxLQUFLOzhCQUNMLEtBQUs7MEJBQ0wsS0FBSzs0QkFDTCxLQUFLO3lCQUNMLEtBQUs7MEJBQ0wsS0FBSzswQkFDTCxLQUFLO3NCQUNMLFlBQVksU0FBQyxrQkFBa0I7O0lBVFA7UUFBZixZQUFZLEVBQUU7O3VEQUFtQjtJQUNsQjtRQUFmLFlBQVksRUFBRTs7c0RBQW1CO0lBQ2xCO1FBQWYsWUFBWSxFQUFFOzt3REFBcUI7SUFZL0Msc0JBQUM7Q0FBQSxBQXBDRCxJQW9DQztTQWZZLGVBQWU7OztJQUMxQixxQ0FBMkM7O0lBQzNDLG9DQUEyQzs7SUFDM0Msc0NBQTZDOztJQUM3QyxzQ0FBZ0Q7O0lBQ2hELGtDQUFvQzs7SUFDcEMsb0NBQWtEOztJQUNsRCxpQ0FBd0I7O0lBQ3hCLGtDQUE2Qzs7SUFDN0Msa0NBQTZDOztJQUM3Qyw4QkFBMEQiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBSZW5kZXJlcjIsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56Q2FyZFRhYkNvbXBvbmVudCB9IGZyb20gJy4vbnotY2FyZC10YWIuY29tcG9uZW50JztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotY2FyZCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWNhcmQuY29tcG9uZW50Lmh0bWwnLFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1jYXJkIHtcbiAgICAgICAgZGlzcGxheTogYmxvY2s7XG4gICAgICB9XG4gICAgYFxuICBdLFxuICBob3N0OiB7XG4gICAgJ1tjbGFzcy5hbnQtY2FyZC1sb2FkaW5nXSc6ICduekxvYWRpbmcnLFxuICAgICdbY2xhc3MuYW50LWNhcmQtYm9yZGVyZWRdJzogJ256Qm9yZGVyZWQnLFxuICAgICdbY2xhc3MuYW50LWNhcmQtaG92ZXJhYmxlXSc6ICduekhvdmVyYWJsZScsXG4gICAgJ1tjbGFzcy5hbnQtY2FyZC10eXBlLWlubmVyXSc6IGBuelR5cGUgPT09ICdpbm5lcidgLFxuICAgICdbY2xhc3MuYW50LWNhcmQtY29udGFpbi10YWJzXSc6ICchIXRhYidcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOekNhcmRDb21wb25lbnQge1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpCb3JkZXJlZCA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekxvYWRpbmcgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56SG92ZXJhYmxlID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56Qm9keVN0eWxlOiB7IFtrZXk6IHN0cmluZ106IHN0cmluZyB9O1xuICBASW5wdXQoKSBuekNvdmVyOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpBY3Rpb25zOiBBcnJheTxUZW1wbGF0ZVJlZjx2b2lkPj4gPSBbXTtcbiAgQElucHV0KCkgbnpUeXBlOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56VGl0bGU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuekV4dHJhOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQENvbnRlbnRDaGlsZChOekNhcmRUYWJDb21wb25lbnQpIHRhYjogTnpDYXJkVGFiQ29tcG9uZW50O1xuXG4gIGNvbnN0cnVjdG9yKHJlbmRlcmVyOiBSZW5kZXJlcjIsIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYpIHtcbiAgICByZW5kZXJlci5hZGRDbGFzcyhlbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdhbnQtY2FyZCcpO1xuICB9XG59XG4iXX0=