/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, ElementRef, EventEmitter, Input, Output, Renderer2, ViewEncapsulation } from '@angular/core';
import { fadeMotion } from '../core/animation/fade';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
export class NzTagComponent {
    /**
     * @param {?} renderer
     * @param {?} elementRef
     * @param {?} nzUpdateHostClassService
     */
    constructor(renderer, elementRef, nzUpdateHostClassService) {
        this.renderer = renderer;
        this.elementRef = elementRef;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.presetColor = false;
        this.nzMode = 'default';
        this.nzChecked = false;
        this.nzNoAnimation = false;
        this.nzAfterClose = new EventEmitter();
        this.nzOnClose = new EventEmitter();
        this.nzCheckedChange = new EventEmitter();
    }
    /**
     * @private
     * @param {?=} color
     * @return {?}
     */
    isPresetColor(color) {
        if (!color) {
            return false;
        }
        return /^(pink|red|yellow|orange|cyan|green|blue|purple|geekblue|magenta|volcano|gold|lime)(-inverse)?$/.test(color);
    }
    /**
     * @private
     * @return {?}
     */
    updateClassMap() {
        this.presetColor = this.isPresetColor(this.nzColor);
        /** @type {?} */
        const prefix = 'ant-tag';
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, {
            [`${prefix}`]: true,
            [`${prefix}-has-color`]: this.nzColor && !this.presetColor,
            [`${prefix}-${this.nzColor}`]: this.presetColor,
            [`${prefix}-checkable`]: this.nzMode === 'checkable',
            [`${prefix}-checkable-checked`]: this.nzChecked
        });
    }
    /**
     * @return {?}
     */
    updateCheckedStatus() {
        if (this.nzMode === 'checkable') {
            this.nzChecked = !this.nzChecked;
            this.nzCheckedChange.emit(this.nzChecked);
            this.updateClassMap();
        }
    }
    /**
     * @param {?} e
     * @return {?}
     */
    closeTag(e) {
        this.nzOnClose.emit(e);
        if (!e.defaultPrevented) {
            this.renderer.removeChild(this.renderer.parentNode(this.elementRef.nativeElement), this.elementRef.nativeElement);
        }
    }
    /**
     * @param {?} e
     * @return {?}
     */
    afterAnimation(e) {
        if (e.toState === 'void') {
            this.nzAfterClose.emit();
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.updateClassMap();
    }
    /**
     * @return {?}
     */
    ngOnChanges() {
        this.updateClassMap();
    }
}
NzTagComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-tag',
                preserveWhitespaces: false,
                providers: [NzUpdateHostClassService],
                animations: [fadeMotion],
                template: "<ng-content></ng-content>\n<i nz-icon type=\"close\" *ngIf=\"nzMode==='closeable'\" tabindex=\"-1\" (click)=\"closeTag($event)\"></i>\n",
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                host: {
                    '[@fadeMotion]': '',
                    '(@fadeMotion.done)': 'afterAnimation($event)',
                    '(click)': 'updateCheckedStatus()',
                    '[style.background-color]': 'presetColor? null : nzColor'
                }
            }] }
];
/** @nocollapse */
NzTagComponent.ctorParameters = () => [
    { type: Renderer2 },
    { type: ElementRef },
    { type: NzUpdateHostClassService }
];
NzTagComponent.propDecorators = {
    nzMode: [{ type: Input }],
    nzColor: [{ type: Input }],
    nzChecked: [{ type: Input }],
    nzNoAnimation: [{ type: Input }],
    nzAfterClose: [{ type: Output }],
    nzOnClose: [{ type: Output }],
    nzCheckedChange: [{ type: Output }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Boolean)
], NzTagComponent.prototype, "nzChecked", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Boolean)
], NzTagComponent.prototype, "nzNoAnimation", void 0);
if (false) {
    /** @type {?} */
    NzTagComponent.prototype.presetColor;
    /** @type {?} */
    NzTagComponent.prototype.nzMode;
    /** @type {?} */
    NzTagComponent.prototype.nzColor;
    /** @type {?} */
    NzTagComponent.prototype.nzChecked;
    /** @type {?} */
    NzTagComponent.prototype.nzNoAnimation;
    /** @type {?} */
    NzTagComponent.prototype.nzAfterClose;
    /** @type {?} */
    NzTagComponent.prototype.nzOnClose;
    /** @type {?} */
    NzTagComponent.prototype.nzCheckedChange;
    /**
     * @type {?}
     * @private
     */
    NzTagComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTagComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzTagComponent.prototype.nzUpdateHostClassService;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFnLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJ0YWcvbnotdGFnLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUNBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUdMLE1BQU0sRUFDTixTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxVQUFVLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsd0JBQXdCLEVBQUUsTUFBTSw0Q0FBNEMsQ0FBQztBQUN0RixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFpQnBELE1BQU0sT0FBTyxjQUFjOzs7Ozs7SUFvRHpCLFlBQ1UsUUFBbUIsRUFDbkIsVUFBc0IsRUFDdEIsd0JBQWtEO1FBRmxELGFBQVEsR0FBUixRQUFRLENBQVc7UUFDbkIsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0Qiw2QkFBd0IsR0FBeEIsd0JBQXdCLENBQTBCO1FBdEQ1RCxnQkFBVyxHQUFHLEtBQUssQ0FBQztRQUNYLFdBQU0sR0FBMEMsU0FBUyxDQUFDO1FBRTFDLGNBQVMsR0FBWSxLQUFLLENBQUM7UUFDM0Isa0JBQWEsR0FBWSxLQUFLLENBQUM7UUFDckMsaUJBQVksR0FBRyxJQUFJLFlBQVksRUFBUSxDQUFDO1FBQ3hDLGNBQVMsR0FBRyxJQUFJLFlBQVksRUFBYyxDQUFDO1FBQzNDLG9CQUFlLEdBQUcsSUFBSSxZQUFZLEVBQVcsQ0FBQztJQWdEOUQsQ0FBQzs7Ozs7O0lBOUNJLGFBQWEsQ0FBQyxLQUFjO1FBQ2xDLElBQUksQ0FBQyxLQUFLLEVBQUU7WUFDVixPQUFPLEtBQUssQ0FBQztTQUNkO1FBQ0QsT0FBTyxpR0FBaUcsQ0FBQyxJQUFJLENBQzNHLEtBQUssQ0FDTixDQUFDO0lB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Q0FBd0Q7O0lBQ3hELHNDQUEyRDs7SUFDM0QsbUNBQThEOztJQUM5RCx5Q0FBaUU7Ozs7O0lBNkMvRCxrQ0FBMkI7Ozs7O0lBQzNCLG9DQUE4Qjs7Ozs7SUFDOUIsa0RBQTBEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQW5pbWF0aW9uRXZlbnQgfSBmcm9tICdAYW5ndWxhci9hbmltYXRpb25zJztcbmltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFJlbmRlcmVyMixcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBmYWRlTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vZmFkZSc7XG5pbXBvcnQgeyBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3NlcnZpY2VzL3VwZGF0ZS1ob3N0LWNsYXNzLnNlcnZpY2UnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei10YWcnLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgYW5pbWF0aW9uczogW2ZhZGVNb3Rpb25dLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotdGFnLmNvbXBvbmVudC5odG1sJyxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGhvc3Q6IHtcbiAgICAnW0BmYWRlTW90aW9uXSc6ICcnLFxuICAgICcoQGZhZGVNb3Rpb24uZG9uZSknOiAnYWZ0ZXJBbmltYXRpb24oJGV2ZW50KScsXG4gICAgJyhjbGljayknOiAndXBkYXRlQ2hlY2tlZFN0YXR1cygpJyxcbiAgICAnW3N0eWxlLmJhY2tncm91bmQtY29sb3JdJzogJ3ByZXNldENvbG9yPyBudWxsIDogbnpDb2xvcidcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelRhZ0NvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25DaGFuZ2VzIHtcbiAgcHJlc2V0Q29sb3IgPSBmYWxzZTtcbiAgQElucHV0KCkgbnpNb2RlOiAnZGVmYXVsdCcgfCAnY2xvc2VhYmxlJyB8ICdjaGVja2FibGUnID0gJ2RlZmF1bHQnO1xuICBASW5wdXQoKSBuekNvbG9yOiBzdHJpbmc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekNoZWNrZWQ6IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Tm9BbmltYXRpb246IGJvb2xlYW4gPSBmYWxzZTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56QWZ0ZXJDbG9zZSA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25DbG9zZSA9IG5ldyBFdmVudEVtaXR0ZXI8TW91c2VFdmVudD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56Q2hlY2tlZENoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Ym9vbGVhbj4oKTtcblxuICBwcml2YXRlIGlzUHJlc2V0Q29sb3IoY29sb3I/OiBzdHJpbmcpOiBib29sZWFuIHtcbiAgICBpZiAoIWNvbG9yKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICAgIHJldHVybiAvXihwaW5rfHJlZHx5ZWxsb3d8b3JhbmdlfGN5YW58Z3JlZW58Ymx1ZXxwdXJwbGV8Z2Vla2JsdWV8bWFnZW50YXx2b2xjYW5vfGdvbGR8bGltZSkoLWludmVyc2UpPyQvLnRlc3QoXG4gICAgICBjb2xvclxuICAgICk7XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZUNsYXNzTWFwKCk6IHZvaWQge1xuICAgIHRoaXMucHJlc2V0Q29sb3IgPSB0aGlzLmlzUHJlc2V0Q29sb3IodGhpcy5uekNvbG9yKTtcbiAgICBjb25zdCBwcmVmaXggPSAnYW50LXRhZyc7XG4gICAgdGhpcy5uelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCB7XG4gICAgICBbYCR7cHJlZml4fWBdOiB0cnVlLFxuICAgICAgW2Ake3ByZWZpeH0taGFzLWNvbG9yYF06IHRoaXMubnpDb2xvciAmJiAhdGhpcy5wcmVzZXRDb2xvcixcbiAgICAgIFtgJHtwcmVmaXh9LSR7dGhpcy5uekNvbG9yfWBdOiB0aGlzLnByZXNldENvbG9yLFxuICAgICAgW2Ake3ByZWZpeH0tY2hlY2thYmxlYF06IHRoaXMubnpNb2RlID09PSAnY2hlY2thYmxlJyxcbiAgICAgIFtgJHtwcmVmaXh9LWNoZWNrYWJsZS1jaGVja2VkYF06IHRoaXMubnpDaGVja2VkXG4gICAgfSk7XG4gIH1cblxuICB1cGRhdGVDaGVja2VkU3RhdHVzKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56TW9kZSA9PT0gJ2NoZWNrYWJsZScpIHtcbiAgICAgIHRoaXMubnpDaGVja2VkID0gIXRoaXMubnpDaGVja2VkO1xuICAgICAgdGhpcy5uekNoZWNrZWRDaGFuZ2UuZW1pdCh0aGlzLm56Q2hlY2tlZCk7XG4gICAgICB0aGlzLnVwZGF0ZUNsYXNzTWFwKCk7XG4gICAgfVxuICB9XG5cbiAgY2xvc2VUYWcoZTogTW91c2VFdmVudCk6IHZvaWQge1xuICAgIHRoaXMubnpPbkNsb3NlLmVtaXQoZSk7XG4gICAgaWYgKCFlLmRlZmF1bHRQcmV2ZW50ZWQpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2hpbGQodGhpcy5yZW5kZXJlci5wYXJlbnROb2RlKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50KSwgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQpO1xuICAgIH1cbiAgfVxuXG4gIGFmdGVyQW5pbWF0aW9uKGU6IEFuaW1hdGlvbkV2ZW50KTogdm9pZCB7XG4gICAgaWYgKGUudG9TdGF0ZSA9PT0gJ3ZvaWQnKSB7XG4gICAgICB0aGlzLm56QWZ0ZXJDbG9zZS5lbWl0KCk7XG4gICAgfVxuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIG56VXBkYXRlSG9zdENsYXNzU2VydmljZTogTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXG4gICkge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZUNsYXNzTWFwKCk7XG4gIH1cblxuICBuZ09uQ2hhbmdlcygpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZUNsYXNzTWFwKCk7XG4gIH1cbn1cbiJdfQ==