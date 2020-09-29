/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, Component, ContentChildren, Input, QueryList, ViewEncapsulation } from '@angular/core';
import { NzCommentActionComponent as CommentAction } from './nz-comment-cells';
var NzCommentComponent = /** @class */ (function () {
    function NzCommentComponent() {
    }
    NzCommentComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-comment',
                    template: "<div class=\"ant-comment-inner\">\n  <div class=\"ant-comment-avatar\">\n    <ng-content select=\"nz-avatar[nz-comment-avatar]\"></ng-content>\n  </div>\n  <div class=\"ant-comment-content\">\n    <div class=\"ant-comment-content-author\">\n      <span *ngIf=\"nzAuthor\" class=\"ant-comment-content-author-name\">\n        <ng-container *nzStringTemplateOutlet=\"nzAuthor\">{{ nzAuthor }}</ng-container>\n      </span>\n      <span *ngIf=\"nzDatetime\" class=\"ant-comment-content-author-time\">\n        <ng-container *nzStringTemplateOutlet=\"nzDatetime\">{{ nzDatetime }}</ng-container>\n      </span>\n    </div>\n    <ng-content select=\"nz-comment-content\"></ng-content>\n    <ul class=\"ant-comment-actions\" *ngIf=\"actions?.length\">\n      <li *ngFor=\"let action of actions\">\n        <span><ng-template [nzCommentActionHost]=\"action.content\"></ng-template></span>\n      </li>\n    </ul>\n  </div>\n</div>\n<div class=\"ant-comment-nested\">\n  <ng-content></ng-content>\n</div>",
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    host: {
                        class: 'ant-comment'
                    },
                    styles: ["\n      nz-comment {\n        display: block;\n      }\n\n      nz-comment-content {\n        display: block;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzCommentComponent.ctorParameters = function () { return []; };
    NzCommentComponent.propDecorators = {
        nzAuthor: [{ type: Input }],
        nzDatetime: [{ type: Input }],
        actions: [{ type: ContentChildren, args: [CommentAction,] }]
    };
    return NzCommentComponent;
}());
export { NzCommentComponent };
if (false) {
    /** @type {?} */
    NzCommentComponent.prototype.nzAuthor;
    /** @type {?} */
    NzCommentComponent.prototype.nzDatetime;
    /** @type {?} */
    NzCommentComponent.prototype.actions;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY29tbWVudC5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29tbWVudC9uei1jb21tZW50LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixTQUFTLEVBQ1QsZUFBZSxFQUNmLEtBQUssRUFDTCxTQUFTLEVBRVQsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSx3QkFBd0IsSUFBSSxhQUFhLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUUvRTtJQXlCRTtJQUFlLENBQUM7O2dCQXpCakIsU0FBUyxTQUFDO29CQUNULFFBQVEsRUFBRSxZQUFZO29CQUN0QiwrK0JBQTBDO29CQUMxQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtvQkFDckMsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07b0JBQy9DLElBQUksRUFBRTt3QkFDSixLQUFLLEVBQUUsYUFBYTtxQkFDckI7NkJBRUMsOEhBUUM7aUJBRUo7Ozs7OzJCQUVFLEtBQUs7NkJBQ0wsS0FBSzswQkFFTCxlQUFlLFNBQUMsYUFBYTs7SUFFaEMseUJBQUM7Q0FBQSxBQTFCRCxJQTBCQztTQU5ZLGtCQUFrQjs7O0lBQzdCLHNDQUE4Qzs7SUFDOUMsd0NBQWdEOztJQUVoRCxxQ0FBa0UiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGRyZW4sXG4gIElucHV0LFxuICBRdWVyeUxpc3QsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IE56Q29tbWVudEFjdGlvbkNvbXBvbmVudCBhcyBDb21tZW50QWN0aW9uIH0gZnJvbSAnLi9uei1jb21tZW50LWNlbGxzJztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotY29tbWVudCcsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1jb21tZW50LmNvbXBvbmVudC5odG1sJyxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGhvc3Q6IHtcbiAgICBjbGFzczogJ2FudC1jb21tZW50J1xuICB9LFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1jb21tZW50IHtcbiAgICAgICAgZGlzcGxheTogYmxvY2s7XG4gICAgICB9XG5cbiAgICAgIG56LWNvbW1lbnQtY29udGVudCB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOekNvbW1lbnRDb21wb25lbnQge1xuICBASW5wdXQoKSBuekF1dGhvcjogc3RyaW5nIHwgVGVtcGxhdGVSZWY8dm9pZD47XG4gIEBJbnB1dCgpIG56RGF0ZXRpbWU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+O1xuXG4gIEBDb250ZW50Q2hpbGRyZW4oQ29tbWVudEFjdGlvbikgYWN0aW9uczogUXVlcnlMaXN0PENvbW1lbnRBY3Rpb24+O1xuICBjb25zdHJ1Y3RvcigpIHt9XG59XG4iXX0=