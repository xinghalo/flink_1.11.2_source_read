/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, Input, ViewEncapsulation } from '@angular/core';
import { InputBoolean, InputNumber } from '../core/util/convert';
/**
 * @record
 */
export function NzSliderTrackStyle() { }
if (false) {
    /** @type {?|undefined} */
    NzSliderTrackStyle.prototype.bottom;
    /** @type {?|undefined} */
    NzSliderTrackStyle.prototype.height;
    /** @type {?|undefined} */
    NzSliderTrackStyle.prototype.left;
    /** @type {?|undefined} */
    NzSliderTrackStyle.prototype.width;
    /** @type {?|undefined} */
    NzSliderTrackStyle.prototype.visibility;
}
export class NzSliderTrackComponent {
    constructor() {
        this.nzVertical = false;
        this.nzIncluded = false;
        this.style = {};
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzIncluded) {
            this.style.visibility = this.nzIncluded ? 'visible' : 'hidden';
        }
        if (changes.nzVertical || changes.nzOffset || changes.nzLength) {
            if (this.nzVertical) {
                this.style.bottom = `${this.nzOffset}%`;
                this.style.height = `${this.nzLength}%`;
                this.style.left = null;
                this.style.width = null;
            }
            else {
                this.style.left = `${this.nzOffset}%`;
                this.style.width = `${this.nzLength}%`;
                this.style.bottom = null;
                this.style.height = null;
            }
        }
    }
}
NzSliderTrackComponent.decorators = [
    { type: Component, args: [{
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                selector: 'nz-slider-track',
                preserveWhitespaces: false,
                template: "<div class=\"ant-slider-track\" [ngStyle]=\"style\"></div>"
            }] }
];
NzSliderTrackComponent.propDecorators = {
    nzOffset: [{ type: Input }],
    nzLength: [{ type: Input }],
    nzVertical: [{ type: Input }],
    nzIncluded: [{ type: Input }]
};
tslib_1.__decorate([
    InputNumber(),
    tslib_1.__metadata("design:type", Number)
], NzSliderTrackComponent.prototype, "nzOffset", void 0);
tslib_1.__decorate([
    InputNumber(),
    tslib_1.__metadata("design:type", Number)
], NzSliderTrackComponent.prototype, "nzLength", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSliderTrackComponent.prototype, "nzVertical", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSliderTrackComponent.prototype, "nzIncluded", void 0);
if (false) {
    /** @type {?} */
    NzSliderTrackComponent.prototype.nzOffset;
    /** @type {?} */
    NzSliderTrackComponent.prototype.nzLength;
    /** @type {?} */
    NzSliderTrackComponent.prototype.nzVertical;
    /** @type {?} */
    NzSliderTrackComponent.prototype.nzIncluded;
    /** @type {?} */
    NzSliderTrackComponent.prototype.style;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2xpZGVyLXRyYWNrLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzbGlkZXIvbnotc2xpZGVyLXRyYWNrLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxTQUFTLEVBQUUsS0FBSyxFQUE0QixpQkFBaUIsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUV2SCxPQUFPLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxNQUFNLHNCQUFzQixDQUFDOzs7O0FBRWpFLHdDQU1DOzs7SUFMQyxvQ0FBdUI7O0lBQ3ZCLG9DQUF1Qjs7SUFDdkIsa0NBQXFCOztJQUNyQixtQ0FBc0I7O0lBQ3RCLHdDQUFvQjs7QUFVdEIsTUFBTSxPQUFPLHNCQUFzQjtJQVBuQztRQVUyQixlQUFVLEdBQUcsS0FBSyxDQUFDO1FBQ25CLGVBQVUsR0FBRyxLQUFLLENBQUM7UUFFNUMsVUFBSyxHQUF1QixFQUFFLENBQUM7SUFvQmpDLENBQUM7Ozs7O0lBbEJDLFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxVQUFVLEVBQUU7WUFDdEIsSUFBSSxDQUFDLEtBQUssQ0FBQyxVQUFVLEdBQUcsSUFBSSxDQUFDLFVBQVUsQ0FBQyxDQUFDLENBQUMsU0FBUyxDQUFDLENBQUMsQ0FBQyxRQUFRLENBQUM7U0FDaEU7UUFDRCxJQUFJLE9BQU8sQ0FBQyxVQUFVLElBQUksT0FBTyxDQUFDLFFBQVEsSUFBSSxPQUFPLENBQUMsUUFBUSxFQUFFO1lBQzlELElBQUksSUFBSSxDQUFDLFVBQVUsRUFBRTtnQkFDbkIsSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsR0FBRyxJQUFJLENBQUMsUUFBUSxHQUFHLENBQUM7Z0JBQ3hDLElBQUksQ0FBQyxLQUFLLENBQUMsTUFBTSxHQUFHLEdBQUcsSUFBSSxDQUFDLFFBQVEsR0FBRyxDQUFDO2dCQUN4QyxJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksR0FBRyxJQUFJLENBQUM7Z0JBQ3ZCLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxHQUFHLElBQUksQ0FBQzthQUN6QjtpQkFBTTtnQkFDTCxJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksR0FBRyxHQUFHLElBQUksQ0FBQyxRQUFRLEdBQUcsQ0FBQztnQkFDdEMsSUFBSSxDQUFDLEtBQUssQ0FBQyxLQUFLLEdBQUcsR0FBRyxJQUFJLENBQUMsUUFBUSxHQUFHLENBQUM7Z0JBQ3ZDLElBQUksQ0FBQyxLQUFLLENBQUMsTUFBTSxHQUFHLElBQUksQ0FBQztnQkFDekIsSUFBSSxDQUFDLEtBQUssQ0FBQyxNQUFNLEdBQUcsSUFBSSxDQUFDO2FBQzFCO1NBQ0Y7SUFDSCxDQUFDOzs7WUFoQ0YsU0FBUyxTQUFDO2dCQUNULGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtnQkFDckMsUUFBUSxFQUFFLGlCQUFpQjtnQkFDM0IsbUJBQW1CLEVBQUUsS0FBSztnQkFDMUIsc0VBQStDO2FBQ2hEOzs7dUJBRUUsS0FBSzt1QkFDTCxLQUFLO3lCQUNMLEtBQUs7eUJBQ0wsS0FBSzs7QUFIa0I7SUFBZCxXQUFXLEVBQUU7O3dEQUFrQjtBQUNqQjtJQUFkLFdBQVcsRUFBRTs7d0RBQWtCO0FBQ2hCO0lBQWYsWUFBWSxFQUFFOzswREFBb0I7QUFDbkI7SUFBZixZQUFZLEVBQUU7OzBEQUFvQjs7O0lBSDVDLDBDQUF5Qzs7SUFDekMsMENBQXlDOztJQUN6Qyw0Q0FBNEM7O0lBQzVDLDRDQUE0Qzs7SUFFNUMsdUNBQStCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksIENvbXBvbmVudCwgSW5wdXQsIE9uQ2hhbmdlcywgU2ltcGxlQ2hhbmdlcywgVmlld0VuY2Fwc3VsYXRpb24gfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgSW5wdXRCb29sZWFuLCBJbnB1dE51bWJlciB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuZXhwb3J0IGludGVyZmFjZSBOelNsaWRlclRyYWNrU3R5bGUge1xuICBib3R0b20/OiBzdHJpbmcgfCBudWxsO1xuICBoZWlnaHQ/OiBzdHJpbmcgfCBudWxsO1xuICBsZWZ0Pzogc3RyaW5nIHwgbnVsbDtcbiAgd2lkdGg/OiBzdHJpbmcgfCBudWxsO1xuICB2aXNpYmlsaXR5Pzogc3RyaW5nO1xufVxuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotc2xpZGVyLXRyYWNrJyxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1zbGlkZXItdHJhY2suY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56U2xpZGVyVHJhY2tDb21wb25lbnQgaW1wbGVtZW50cyBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuek9mZnNldDogbnVtYmVyO1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuekxlbmd0aDogbnVtYmVyO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpWZXJ0aWNhbCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpJbmNsdWRlZCA9IGZhbHNlO1xuXG4gIHN0eWxlOiBOelNsaWRlclRyYWNrU3R5bGUgPSB7fTtcblxuICBuZ09uQ2hhbmdlcyhjaGFuZ2VzOiBTaW1wbGVDaGFuZ2VzKTogdm9pZCB7XG4gICAgaWYgKGNoYW5nZXMubnpJbmNsdWRlZCkge1xuICAgICAgdGhpcy5zdHlsZS52aXNpYmlsaXR5ID0gdGhpcy5uekluY2x1ZGVkID8gJ3Zpc2libGUnIDogJ2hpZGRlbic7XG4gICAgfVxuICAgIGlmIChjaGFuZ2VzLm56VmVydGljYWwgfHwgY2hhbmdlcy5uek9mZnNldCB8fCBjaGFuZ2VzLm56TGVuZ3RoKSB7XG4gICAgICBpZiAodGhpcy5uelZlcnRpY2FsKSB7XG4gICAgICAgIHRoaXMuc3R5bGUuYm90dG9tID0gYCR7dGhpcy5uek9mZnNldH0lYDtcbiAgICAgICAgdGhpcy5zdHlsZS5oZWlnaHQgPSBgJHt0aGlzLm56TGVuZ3RofSVgO1xuICAgICAgICB0aGlzLnN0eWxlLmxlZnQgPSBudWxsO1xuICAgICAgICB0aGlzLnN0eWxlLndpZHRoID0gbnVsbDtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc3R5bGUubGVmdCA9IGAke3RoaXMubnpPZmZzZXR9JWA7XG4gICAgICAgIHRoaXMuc3R5bGUud2lkdGggPSBgJHt0aGlzLm56TGVuZ3RofSVgO1xuICAgICAgICB0aGlzLnN0eWxlLmJvdHRvbSA9IG51bGw7XG4gICAgICAgIHRoaXMuc3R5bGUuaGVpZ2h0ID0gbnVsbDtcbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cbiJdfQ==