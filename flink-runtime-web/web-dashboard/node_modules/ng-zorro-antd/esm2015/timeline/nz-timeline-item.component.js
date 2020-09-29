/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, Renderer2, ViewChild, ViewEncapsulation } from '@angular/core';
export class NzTimelineItemComponent {
    /**
     * @param {?} renderer
     * @param {?} cdr
     */
    constructor(renderer, cdr) {
        this.renderer = renderer;
        this.cdr = cdr;
        this.nzColor = 'blue';
        this.isLast = false;
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.tryUpdateCustomColor();
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzColor) {
            this.tryUpdateCustomColor();
        }
    }
    /**
     * @return {?}
     */
    detectChanges() {
        this.cdr.detectChanges();
    }
    /**
     * @private
     * @return {?}
     */
    tryUpdateCustomColor() {
        /** @type {?} */
        const defaultColors = ['blue', 'red', 'green'];
        /** @type {?} */
        const circle = this.liTemplate.nativeElement.querySelector('.ant-timeline-item-head');
        if (defaultColors.indexOf(this.nzColor) === -1) {
            this.renderer.setStyle(circle, 'border-color', this.nzColor);
        }
        else {
            this.renderer.removeStyle(circle, 'border-color');
        }
    }
}
NzTimelineItemComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                preserveWhitespaces: false,
                selector: 'nz-timeline-item, [nz-timeline-item]',
                template: "<li\n  class=\"ant-timeline-item\"\n  [class.ant-timeline-item-right]=\"position === 'right'\"\n  [class.ant-timeline-item-left]=\"position === 'left'\"\n  [class.ant-timeline-item-last]=\"isLast\"\n  #liTemplate>\n  <div class=\"ant-timeline-item-tail\"></div>\n  <div\n    class=\"ant-timeline-item-head\"\n    [class.ant-timeline-item-head-red]=\"nzColor === 'red'\"\n    [class.ant-timeline-item-head-blue]=\"nzColor === 'blue'\"\n    [class.ant-timeline-item-head-green]=\"nzColor === 'green'\"\n    [class.ant-timeline-item-head-custom]=\"!!nzDot\">\n    <ng-container *nzStringTemplateOutlet=\"nzDot\">{{ nzDot }}</ng-container>\n  </div>\n  <div class=\"ant-timeline-item-content\">\n    <ng-content></ng-content>\n  </div>\n</li>"
            }] }
];
/** @nocollapse */
NzTimelineItemComponent.ctorParameters = () => [
    { type: Renderer2 },
    { type: ChangeDetectorRef }
];
NzTimelineItemComponent.propDecorators = {
    liTemplate: [{ type: ViewChild, args: ['liTemplate',] }],
    nzColor: [{ type: Input }],
    nzDot: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzTimelineItemComponent.prototype.liTemplate;
    /** @type {?} */
    NzTimelineItemComponent.prototype.nzColor;
    /** @type {?} */
    NzTimelineItemComponent.prototype.nzDot;
    /** @type {?} */
    NzTimelineItemComponent.prototype.isLast;
    /** @type {?} */
    NzTimelineItemComponent.prototype.position;
    /**
     * @type {?}
     * @private
     */
    NzTimelineItemComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTimelineItemComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGltZWxpbmUtaXRlbS5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidGltZWxpbmUvbnotdGltZWxpbmUtaXRlbS5jb21wb25lbnQudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsS0FBSyxFQUdMLFNBQVMsRUFHVCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBV3ZCLE1BQU0sT0FBTyx1QkFBdUI7Ozs7O0lBUWxDLFlBQW9CLFFBQW1CLEVBQVUsR0FBc0I7UUFBbkQsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQUFVLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBTjlELFlBQU8sR0FBVyxNQUFNLENBQUM7UUFHbEMsV0FBTSxHQUFHLEtBQUssQ0FBQztJQUcyRCxDQUFDOzs7O0lBRTNFLFFBQVE7UUFDTixJQUFJLENBQUMsb0JBQW9CLEVBQUUsQ0FBQztJQUM5QixDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxPQUFPLEVBQUU7WUFDbkIsSUFBSSxDQUFDLG9CQUFvQixFQUFFLENBQUM7U0FDN0I7SUFDSCxDQUFDOzs7O0lBRUQsYUFBYTtRQUNYLElBQUksQ0FBQyxHQUFHLENBQUMsYUFBYSxFQUFFLENBQUM7SUFDM0IsQ0FBQzs7Ozs7SUFFTyxvQkFBb0I7O2NBQ3BCLGFBQWEsR0FBRyxDQUFDLE1BQU0sRUFBRSxLQUFLLEVBQUUsT0FBTyxDQUFDOztjQUN4QyxNQUFNLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUMsYUFBYSxDQUFDLHlCQUF5QixDQUFDO1FBQ3JGLElBQUksYUFBYSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUU7WUFDOUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsTUFBTSxFQUFFLGNBQWMsRUFBRSxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUM7U0FDOUQ7YUFBTTtZQUNMLElBQUksQ0FBQyxRQUFRLENBQUMsV0FBVyxDQUFDLE1BQU0sRUFBRSxjQUFjLENBQUMsQ0FBQztTQUNuRDtJQUNILENBQUM7OztZQXZDRixTQUFTLFNBQUM7Z0JBQ1QsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07Z0JBQy9DLGFBQWEsRUFBRSxpQkFBaUIsQ0FBQyxJQUFJO2dCQUNyQyxtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQixRQUFRLEVBQUUsc0NBQXNDO2dCQUNoRCw4dUJBQWdEO2FBQ2pEOzs7O1lBZkMsU0FBUztZQU5ULGlCQUFpQjs7O3lCQXVCaEIsU0FBUyxTQUFDLFlBQVk7c0JBQ3RCLEtBQUs7b0JBQ0wsS0FBSzs7OztJQUZOLDZDQUFnRDs7SUFDaEQsMENBQWtDOztJQUNsQyx3Q0FBMkM7O0lBRTNDLHlDQUFlOztJQUNmLDJDQUFxQzs7Ozs7SUFFekIsMkNBQTJCOzs7OztJQUFFLHNDQUE4QiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBPbkluaXQsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56VGltZWxpbmVNb2RlIH0gZnJvbSAnLi9uei10aW1lbGluZS5jb21wb25lbnQnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBzZWxlY3RvcjogJ256LXRpbWVsaW5lLWl0ZW0sIFtuei10aW1lbGluZS1pdGVtXScsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10aW1lbGluZS1pdGVtLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOelRpbWVsaW5lSXRlbUNvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25DaGFuZ2VzIHtcbiAgQFZpZXdDaGlsZCgnbGlUZW1wbGF0ZScpIGxpVGVtcGxhdGU6IEVsZW1lbnRSZWY7XG4gIEBJbnB1dCgpIG56Q29sb3I6IHN0cmluZyA9ICdibHVlJztcbiAgQElucHV0KCkgbnpEb3Q6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIGlzTGFzdCA9IGZhbHNlO1xuICBwb3NpdGlvbjogTnpUaW1lbGluZU1vZGUgfCB1bmRlZmluZWQ7XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyLCBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy50cnlVcGRhdGVDdXN0b21Db2xvcigpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLm56Q29sb3IpIHtcbiAgICAgIHRoaXMudHJ5VXBkYXRlQ3VzdG9tQ29sb3IoKTtcbiAgICB9XG4gIH1cblxuICBkZXRlY3RDaGFuZ2VzKCk6IHZvaWQge1xuICAgIHRoaXMuY2RyLmRldGVjdENoYW5nZXMoKTtcbiAgfVxuXG4gIHByaXZhdGUgdHJ5VXBkYXRlQ3VzdG9tQ29sb3IoKTogdm9pZCB7XG4gICAgY29uc3QgZGVmYXVsdENvbG9ycyA9IFsnYmx1ZScsICdyZWQnLCAnZ3JlZW4nXTtcbiAgICBjb25zdCBjaXJjbGUgPSB0aGlzLmxpVGVtcGxhdGUubmF0aXZlRWxlbWVudC5xdWVyeVNlbGVjdG9yKCcuYW50LXRpbWVsaW5lLWl0ZW0taGVhZCcpO1xuICAgIGlmIChkZWZhdWx0Q29sb3JzLmluZGV4T2YodGhpcy5uekNvbG9yKSA9PT0gLTEpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUoY2lyY2xlLCAnYm9yZGVyLWNvbG9yJywgdGhpcy5uekNvbG9yKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVTdHlsZShjaXJjbGUsICdib3JkZXItY29sb3InKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==